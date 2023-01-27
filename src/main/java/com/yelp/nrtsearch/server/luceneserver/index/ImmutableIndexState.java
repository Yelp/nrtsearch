/*
 * Copyright 2022 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.luceneserver.index;

import static com.yelp.nrtsearch.server.luceneserver.BackupIndexRequestHandler.getSegmentFilesInSnapshot;
import static com.yelp.nrtsearch.server.luceneserver.BackupIndexRequestHandler.releaseSnapshot;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.SnapshotId;
import com.yelp.nrtsearch.server.luceneserver.CreateSnapshotHandler;
import com.yelp.nrtsearch.server.luceneserver.DirectoryFactory;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler.SearchHandlerException;
import com.yelp.nrtsearch.server.luceneserver.ServerCodec;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.luceneserver.search.SortParser;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of index state which is immutable. */
public class ImmutableIndexState extends IndexState {
  private static final Logger logger = LoggerFactory.getLogger(ImmutableIndexState.class);
  private static final EnumSet<Type> ALLOWED_INDEX_SORT_TYPES =
      EnumSet.of(
          SortField.Type.STRING,
          SortField.Type.LONG,
          SortField.Type.INT,
          SortField.Type.DOUBLE,
          SortField.Type.FLOAT);

  public static final double DEFAULT_NRT_CACHING_MAX_MERGE_SIZE_MB = 5.0;
  public static final double DEFAULT_NRT_CACHING_MAX_SIZE_MB = 60.0;
  public static final boolean DEFAULT_MERGE_AUTO_THROTTLE = false;
  public static final String DEFAULT_DIRECTORY = "FSDirectory";

  // default settings as message, so they can be merged with saved settings
  public static final IndexSettings DEFAULT_INDEX_SETTINGS =
      IndexSettings.newBuilder()
          .setNrtCachingDirectoryMaxMergeSizeMB(
              DoubleValue.newBuilder().setValue(DEFAULT_NRT_CACHING_MAX_MERGE_SIZE_MB).build())
          .setNrtCachingDirectoryMaxSizeMB(
              DoubleValue.newBuilder().setValue(DEFAULT_NRT_CACHING_MAX_SIZE_MB).build())
          .setConcurrentMergeSchedulerMaxMergeCount(
              Int32Value.newBuilder()
                  .setValue(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS)
                  .build())
          .setConcurrentMergeSchedulerMaxThreadCount(
              Int32Value.newBuilder()
                  .setValue(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS)
                  .build())
          .setIndexMergeSchedulerAutoThrottle(
              BoolValue.newBuilder().setValue(DEFAULT_MERGE_AUTO_THROTTLE).build())
          .setDirectory(StringValue.newBuilder().setValue(DEFAULT_DIRECTORY).build())
          .build();

  // Settings
  private final double nrtCachingDirectoryMaxMergeSizeMB;
  private final double nrtCachingDirectoryMaxSizeMB;
  private final int concurrentMergeSchedulerMaxThreadCount;
  private final int concurrentMergeSchedulerMaxMergeCount;
  private final Sort indexSort;
  private final boolean indexMergeSchedulerAutoThrottle;
  private final DirectoryFactory directoryFactory;

  public static final double DEFAULT_MAX_REFRESH_SEC = 1.0;
  public static final double DEFAULT_MIN_REFRESH_SEC = 0.05;
  public static final double DEFAULT_MAX_SEARCHER_AGE = 60.0;
  public static final double DEFAULT_INDEX_RAM_BUFFER_SIZE_MB = 16.0;
  public static final int DEFAULT_ADD_DOCS_MAX_BUFFER_LEN = 100;
  public static final int DEFAULT_SLICE_MAX_DOCS = 250_000;
  public static final int DEFAULT_SLICE_MAX_SEGMENTS = 5;
  public static final int DEFAULT_VIRTUAL_SHARDS = 1;
  public static final int DEFAULT_SEGMENTS_PER_TIER = 10;
  public static final int DEFAULT_MAX_MERGED_SEGMENT_MB = 5 * 1024;

  // default live settings as message, so they can be merged with saved settings
  public static final IndexLiveSettings DEFAULT_INDEX_LIVE_SETTINGS =
      IndexLiveSettings.newBuilder()
          .setMaxRefreshSec(DoubleValue.newBuilder().setValue(DEFAULT_MAX_REFRESH_SEC).build())
          .setMinRefreshSec(DoubleValue.newBuilder().setValue(DEFAULT_MIN_REFRESH_SEC).build())
          .setMaxSearcherAgeSec(DoubleValue.newBuilder().setValue(DEFAULT_MAX_SEARCHER_AGE).build())
          .setIndexRamBufferSizeMB(
              DoubleValue.newBuilder().setValue(DEFAULT_INDEX_RAM_BUFFER_SIZE_MB).build())
          .setAddDocumentsMaxBufferLen(
              Int32Value.newBuilder().setValue(DEFAULT_ADD_DOCS_MAX_BUFFER_LEN).build())
          .setSliceMaxDocs(Int32Value.newBuilder().setValue(DEFAULT_SLICE_MAX_DOCS).build())
          .setSliceMaxSegments(Int32Value.newBuilder().setValue(DEFAULT_SLICE_MAX_SEGMENTS).build())
          .setVirtualShards(Int32Value.newBuilder().setValue(DEFAULT_VIRTUAL_SHARDS).build())
          .setSegmentsPerTier(Int32Value.newBuilder().setValue(DEFAULT_SEGMENTS_PER_TIER).build())
          .setMaxMergedSegmentMB(
              Int32Value.newBuilder().setValue(DEFAULT_MAX_MERGED_SEGMENT_MB).build())
          // default unset
          .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(0).build())
          .setDefaultSearchTimeoutCheckEvery(Int32Value.newBuilder().setValue(0).build())
          .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(0).build())
          .setMaxMergePreCopyDurationSec(UInt64Value.newBuilder().setValue(0))
          .build();

  // Live Settings
  private final double maxRefreshSec;
  private final double minRefreshSec;
  private final double maxSearcherAgeSec;
  private final double indexRamBufferSizeMB;
  private final int addDocumentsMaxBufferLen;
  private final int sliceMaxDocs;
  private final int sliceMaxSegments;
  private final int virtualShards;
  private final int maxMergedSegmentMB;
  private final int segmentsPerTier;
  private final double defaultSearchTimeoutSec;
  private final int defaultSearchTimeoutCheckEvery;
  private final int defaultTerminateAfter;
  private final long maxMergePreCopyDurationSec;

  private final IndexStateManager indexStateManager;
  private final String uniqueName;
  private final IndexStateInfo currentStateInfo;
  private final IndexSettings mergedSettings;
  private final IndexLiveSettings mergedLiveSettings;
  private final FieldAndFacetState fieldAndFacetState;
  private final Map<Integer, ShardState> shards;

  /**
   * Constructor.
   *
   * @param indexStateManager state manager for index
   * @param globalState global state
   * @param name index name
   * @param uniqueName index name with instance identifier
   * @param stateInfo current settings state
   * @param fieldAndFacetState current field state
   * @param previousShardState shard state from previous index state, or null
   * @throws IOException on file system error
   */
  public ImmutableIndexState(
      IndexStateManager indexStateManager,
      GlobalState globalState,
      String name,
      String uniqueName,
      IndexStateInfo stateInfo,
      FieldAndFacetState fieldAndFacetState,
      Map<Integer, ShardState> previousShardState)
      throws IOException {
    super(globalState, name, globalState.getIndexDirBase().resolve(uniqueName));
    this.indexStateManager = indexStateManager;
    this.uniqueName = uniqueName;
    this.currentStateInfo = stateInfo;
    this.fieldAndFacetState = fieldAndFacetState;

    // settings
    mergedSettings = mergeSettings(DEFAULT_INDEX_SETTINGS, stateInfo.getSettings());
    validateSettings(mergedSettings);

    nrtCachingDirectoryMaxMergeSizeMB =
        mergedSettings.getNrtCachingDirectoryMaxMergeSizeMB().getValue();
    nrtCachingDirectoryMaxSizeMB = mergedSettings.getNrtCachingDirectoryMaxSizeMB().getValue();
    concurrentMergeSchedulerMaxMergeCount =
        mergedSettings.getConcurrentMergeSchedulerMaxMergeCount().getValue();
    concurrentMergeSchedulerMaxThreadCount =
        mergedSettings.getConcurrentMergeSchedulerMaxThreadCount().getValue();

    if (mergedSettings.hasIndexSort()
        && !mergedSettings.getIndexSort().getSortedFieldsList().isEmpty()) {
      try {
        indexSort =
            SortParser.parseSort(
                mergedSettings.getIndexSort().getSortedFieldsList(),
                null,
                fieldAndFacetState.getFields());
        validateIndexSort(indexSort);
      } catch (SearchHandlerException e) {
        throw new RuntimeException("Error parsing index sort", e);
      }
    } else {
      indexSort = null;
    }
    indexMergeSchedulerAutoThrottle =
        mergedSettings.getIndexMergeSchedulerAutoThrottle().getValue();
    directoryFactory = DirectoryFactory.get(mergedSettings.getDirectory().getValue());

    // live settings
    mergedLiveSettings =
        mergeLiveSettings(DEFAULT_INDEX_LIVE_SETTINGS, stateInfo.getLiveSettings());
    validateLiveSettings(mergedLiveSettings);

    maxRefreshSec = mergedLiveSettings.getMaxRefreshSec().getValue();
    minRefreshSec = mergedLiveSettings.getMinRefreshSec().getValue();
    maxSearcherAgeSec = mergedLiveSettings.getMaxSearcherAgeSec().getValue();
    indexRamBufferSizeMB = mergedLiveSettings.getIndexRamBufferSizeMB().getValue();
    addDocumentsMaxBufferLen = mergedLiveSettings.getAddDocumentsMaxBufferLen().getValue();
    sliceMaxDocs = mergedLiveSettings.getSliceMaxDocs().getValue();
    sliceMaxSegments = mergedLiveSettings.getSliceMaxSegments().getValue();
    virtualShards = mergedLiveSettings.getVirtualShards().getValue();
    maxMergedSegmentMB = mergedLiveSettings.getMaxMergedSegmentMB().getValue();
    segmentsPerTier = mergedLiveSettings.getSegmentsPerTier().getValue();
    defaultSearchTimeoutSec = mergedLiveSettings.getDefaultSearchTimeoutSec().getValue();
    defaultSearchTimeoutCheckEvery =
        mergedLiveSettings.getDefaultSearchTimeoutCheckEvery().getValue();
    defaultTerminateAfter = mergedLiveSettings.getDefaultTerminateAfter().getValue();
    maxMergePreCopyDurationSec = mergedLiveSettings.getMaxMergePreCopyDurationSec().getValue();

    // If there is previous shard state, use it. Otherwise, initialize the shard.
    if (previousShardState != null) {
      shards = previousShardState;
    } else {
      // this is immutable and passed to all subsequent index state objects, can be used
      // for index level locking
      shards =
          ImmutableMap.<Integer, ShardState>builder()
              .put(
                  0,
                  new ShardState(
                      indexStateManager,
                      getName(),
                      getRootDir(),
                      getSearchThreadPoolExecutor(),
                      0,
                      !currentStateInfo.getCommitted()))
              .build();
    }
  }

  /**
   * Update a {@link IndexSettings} message by merging it with another. Only looks at top level
   * fields. If the updates message has a field set, it replaces the field value in base.
   *
   * @param base base message value
   * @param updates message with field updates
   * @return merged settings message
   * @throws IllegalArgumentException if attempting to change index sort
   */
  public static IndexSettings mergeSettings(IndexSettings base, IndexSettings updates) {
    if (base.hasIndexSort()
        && updates.hasIndexSort()
        && !base.getIndexSort().equals(updates.getIndexSort())) {
      throw new IllegalArgumentException("Cannot change index sort value once set");
    }

    FieldMaskUtil.MergeOptions mergeOptions =
        new FieldMaskUtil.MergeOptions().setReplaceMessageFields(true);

    List<String> settingsFields =
        updates.getAllFields().keySet().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toList());
    IndexSettings.Builder mergedSettingBuilder = base.toBuilder();
    FieldMaskUtil.merge(
        FieldMaskUtil.fromStringList(IndexSettings.class, settingsFields),
        updates,
        mergedSettingBuilder,
        mergeOptions);
    return mergedSettingBuilder.build();
  }

  /**
   * Update a {@link IndexLiveSettings} message by merging it with another. Only looks at top level
   * fields. If the updates message has a field set, it replaces the field value in base.
   *
   * @param base base message value
   * @param updates message with field updates
   * @return merged live settings message
   */
  public static IndexLiveSettings mergeLiveSettings(
      IndexLiveSettings base, IndexLiveSettings updates) {
    FieldMaskUtil.MergeOptions mergeOptions =
        new FieldMaskUtil.MergeOptions().setReplaceMessageFields(true);

    List<String> liveSettingsFields =
        updates.getAllFields().keySet().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toList());
    IndexLiveSettings.Builder mergedLiveSettingBuilder = base.toBuilder();
    FieldMaskUtil.merge(
        FieldMaskUtil.fromStringList(IndexLiveSettings.class, liveSettingsFields),
        updates,
        mergedLiveSettingBuilder,
        mergeOptions);
    return mergedLiveSettingBuilder.build();
  }

  /** Get the current index state message representation. */
  public IndexStateInfo getCurrentStateInfo() {
    return currentStateInfo;
  }

  /** Get the fully merged (with defaults) index settings. */
  public IndexSettings getMergedSettings() {
    return mergedSettings;
  }

  /** Get the fully merged (with defaults) index live settings. */
  public IndexLiveSettings getMergedLiveSettings() {
    return mergedLiveSettings;
  }

  /** Get field and facet state for index. */
  public FieldAndFacetState getFieldAndFacetState() {
    return fieldAndFacetState;
  }

  @Override
  public boolean isStarted() {
    for (ShardState shard : shards.values()) {
      if (shard.isStarted()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void start(
      Mode serverMode, Path dataPath, long primaryGen, ReplicationServerClient primaryClient)
      throws IOException {
    if (isStarted()) {
      throw new IllegalStateException("index \"" + getName() + "\" was already started");
    }

    // restore data if provided
    if (dataPath != null) {
      restoreIndexData(dataPath, getRootDir());
    }

    // only create if the index has not been committed
    for (ShardState shard : shards.values()) {
      shard.setDoCreate(!currentStateInfo.getCommitted() && dataPath == null);
    }

    // start all local shards
    switch (serverMode) {
      case STANDALONE:
        for (ShardState shard : shards.values()) {
          shard.start();
        }
        break;
      case PRIMARY:
        for (ShardState shard : shards.values()) {
          shard.startPrimary(primaryGen);
        }
        break;
      case REPLICA:
        for (ShardState shard : shards.values()) {
          shard.startReplica(primaryClient, primaryGen);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown server mode: " + serverMode);
    }
  }

  static void restoreIndexData(Path restorePath, Path indexDataRoot) throws IOException {
    Objects.requireNonNull(restorePath);
    Objects.requireNonNull(indexDataRoot);
    logger.info("Restore index data from path: " + restorePath + " to " + indexDataRoot);
    File restorePathFile = restorePath.toFile();
    File indexDataRootFile = indexDataRoot.toFile();
    if (!restorePath.toFile().exists()) {
      throw new IllegalArgumentException("Restore path does not exist: " + restorePath);
    }
    if (!restorePathFile.isDirectory()) {
      throw new IllegalArgumentException("Restore path is not a directory: " + restorePath);
    }
    if (!indexDataRootFile.exists()) {
      throw new IllegalArgumentException("Index data root path does not exist: " + indexDataRoot);
    }
    if (!indexDataRootFile.isDirectory()) {
      throw new IllegalArgumentException(
          "Index data root path is not a directory: " + indexDataRoot);
    }
    Path restoredDataRoot;
    try (Stream<Path> restorePathFiles = Files.list(restorePath)) {
      restoredDataRoot =
          restorePathFiles
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException("No data in restored directory: " + restorePath))
              .resolve(ShardState.getShardDirectoryName(0))
              .resolve(ShardState.INDEX_DATA_DIR_NAME);
    }
    File restoredDataRootFile = restoredDataRoot.toFile();
    if (!restoredDataRootFile.exists()) {
      throw new IllegalArgumentException(
          "Index data not present in restored directory: " + restorePath);
    }
    if (!restoredDataRootFile.isDirectory()) {
      throw new IllegalArgumentException(
          "Restored index data root is not a directory: " + restorePath);
    }
    if (restoredDataRootFile.listFiles().length == 0) {
      throw new IllegalArgumentException("No index data present in restore: " + restorePath);
    }
    Path destDataRoot =
        indexDataRoot
            .resolve(ShardState.getShardDirectoryName(0))
            .resolve(ShardState.INDEX_DATA_DIR_NAME);
    StateUtils.ensureDirectory(destDataRoot);
    try (Stream<Path> destDataFilesStream = Files.list(destDataRoot)) {
      for (Path p : (Iterable<Path>) destDataFilesStream::iterator) {
        // the lock file will not be part of the restore, so it should be ok to keep it
        if (IndexWriter.WRITE_LOCK_NAME.equals(p.getFileName().toString())) {
          continue;
        }
        throw new IllegalArgumentException("Cannot restore, directory has index data file: " + p);
      }
    }
    // hard link all index files, should this be recursive?
    try (Stream<Path> restoredDataFilesStream = Files.list(restoredDataRoot)) {
      for (Path p : (Iterable<Path>) restoredDataFilesStream::iterator) {
        Path destFile = destDataRoot.resolve(p.getFileName());
        Files.createLink(destFile, p);
      }
    }
  }

  @Override
  public FieldDef getField(String fieldName) {
    FieldDef fd = getMetaFields().get(fieldName);
    if (fd != null) {
      return fd;
    }
    fd = fieldAndFacetState.getFields().get(fieldName);
    if (fd == null) {
      String message =
          "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      throw new IllegalArgumentException(message);
    }
    return fd;
  }

  @Override
  public Map<String, FieldDef> getAllFields() {
    return fieldAndFacetState.getFields();
  }

  @Override
  public String getAllFieldsJSON() {
    JsonObject fieldsRoot = new JsonObject();
    for (Map.Entry<String, Field> entry : currentStateInfo.getFieldsMap().entrySet()) {
      String fieldString;
      try {
        fieldString = JsonFormat.printer().print(entry.getValue());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      JsonElement parsedJsonElement = JsonParser.parseString(fieldString);
      fieldsRoot.add(entry.getKey(), parsedJsonElement);
    }
    return fieldsRoot.toString();
  }

  @Override
  public Optional<IdFieldDef> getIdFieldDef() {
    return fieldAndFacetState.getIdFieldDef();
  }

  @Override
  public List<String> getIndexedAnalyzedFields() {
    return fieldAndFacetState.getIndexedAnalyzedFields();
  }

  @Override
  public Map<String, FieldDef> getEagerGlobalOrdinalFields() {
    return fieldAndFacetState.getEagerGlobalOrdinalFields();
  }

  @Override
  public Map<String, GlobalOrdinalable> getEagerFieldGlobalOrdinalFields() {
    return fieldAndFacetState.getFieldEagerGlobalOrdinalFields();
  }

  @Override
  public Bindings getExpressionBindings() {
    return fieldAndFacetState.getExprBindings();
  }

  @Override
  public boolean hasNestedChildFields() {
    return fieldAndFacetState.getHasNestedChildFields();
  }

  @Override
  public boolean hasFacets() {
    return !fieldAndFacetState.getInternalFacetFieldNames().isEmpty();
  }

  @Override
  public Set<String> getInternalFacetFieldNames() {
    return fieldAndFacetState.getInternalFacetFieldNames();
  }

  @Override
  public FacetsConfig getFacetsConfig() {
    return fieldAndFacetState.getFacetsConfig();
  }

  @Override
  public ShardState getShard(int shardOrd) {
    ShardState shardState = shards.get(shardOrd);
    if (shardState == null) {
      throw new IllegalArgumentException(
          "shardOrd=" + shardOrd + " does not exist in index \"" + getName() + "\"");
    }
    return shardState;
  }

  @Override
  public Map<Integer, ShardState> getShards() {
    return shards;
  }

  @Override
  public long commit(boolean backupFromIncArchiver) throws IOException {
    // the shards map is created once and passed to all subsequent instance
    synchronized (shards) {
      long gen = -1;
      for (ShardState shard : shards.values()) {
        gen = shard.commit();
      }

      SnapshotId snapshotId = null;
      try {
        if (this.getShard(0).isPrimary()
            && getGlobalState().getIncArchiver().isPresent()
            && backupFromIncArchiver) {
          CreateSnapshotRequest createSnapshotRequest =
              CreateSnapshotRequest.newBuilder().setIndexName(getName()).build();

          snapshotId =
              new CreateSnapshotHandler()
                  .createSnapshot(this, createSnapshotRequest)
                  .getSnapshotId();
          // upload data
          Collection<String> segmentFiles = getSegmentFilesInSnapshot(this, snapshotId);
          String resourceData = IndexBackupUtils.getResourceData(uniqueName);
          Archiver incArchiver = getGlobalState().getIncArchiver().get();
          String versionHash =
              incArchiver.upload(
                  getGlobalState().getConfiguration().getServiceName(),
                  resourceData,
                  getRootDir(),
                  segmentFiles,
                  Collections.emptyList(),
                  true);
          incArchiver.blessVersion(
              getGlobalState().getConfiguration().getServiceName(), resourceData, versionHash);
        }
      } finally {
        if (snapshotId != null) {
          releaseSnapshot(this, getName(), snapshotId);
        }
      }
      return gen;
    }
  }

  @Override
  public boolean hasCommit() throws IOException {
    // This method is only used to determine if a snapshot can be taken. We might
    // want to consider moving this logic into CreateSnapshotHandler.
    ShardState shardState = getShard(0);
    return shardState.isStarted() && shardState.snapshots != null;
  }

  @Override
  public void deleteIndex() throws IOException {
    for (ShardState shardState : shards.values()) {
      shardState.deleteShard();
    }
    deleteIndexRootDir();
  }

  @Override
  public IndexWriterConfig getIndexWriterConfig(
      OpenMode openMode, Directory origIndexDir, int shardOrd) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig(new IndexAnalyzer(indexStateManager));
    iwc.setOpenMode(openMode);
    if (getGlobalState().getConfiguration().getIndexVerbose()) {
      logger.info("Enabling verbose logging for Lucene NRT");
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    }

    if (indexSort != null) {
      iwc.setIndexSort(indexSort);
    }

    iwc.setSimilarity(new IndexSimilarity(indexStateManager));
    iwc.setRAMBufferSizeMB(indexRamBufferSizeMB);

    iwc.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(iwc.getInfoStream()));

    ConcurrentMergeScheduler cms =
        new ConcurrentMergeScheduler() {
          @Override
          public synchronized MergeThread getMergeThread(
              IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
            MergeThread thread = super.getMergeThread(writer, merge);
            thread.setName(thread.getName() + " [" + getName() + ":" + shardOrd + "]");
            return thread;
          }
        };
    iwc.setMergeScheduler(cms);

    if (indexMergeSchedulerAutoThrottle) {
      cms.enableAutoIOThrottle();
    } else {
      cms.disableAutoIOThrottle();
    }

    cms.setMaxMergesAndThreads(
        concurrentMergeSchedulerMaxMergeCount, concurrentMergeSchedulerMaxThreadCount);

    iwc.setIndexDeletionPolicy(
        new PersistentSnapshotDeletionPolicy(
            new KeepOnlyLastCommitDeletionPolicy(),
            origIndexDir,
            IndexWriterConfig.OpenMode.CREATE_OR_APPEND));

    iwc.setCodec(new ServerCodec(indexStateManager));

    TieredMergePolicy mergePolicy;
    if (getGlobalState().getConfiguration().getVirtualSharding()) {
      mergePolicy =
          new BucketedTieredMergePolicy(() -> indexStateManager.getCurrent().getVirtualShards());
    } else {
      mergePolicy = new TieredMergePolicy();
    }
    mergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
    mergePolicy.setSegmentsPerTier(segmentsPerTier);
    iwc.setMergePolicy(mergePolicy);

    return iwc;
  }

  // Don't worry about ref counting state versions, we only allow changes that are compatible
  // with existing data. So the latest state should be able to handle older snapshot data.
  @Override
  public long incRefLastCommitGen() throws IOException {
    return -1;
  }

  @Override
  public void decRef(long stateGen) {}

  @Override
  public Map<Long, Integer> getGenRefCounts() {
    return Collections.emptyMap();
  }

  @Override
  public JsonObject getSaveState() {
    String stateString;
    try {
      // Ensure the 'fields' map is always present in the output, even when empty
      stateString =
          JsonFormat.printer()
              .includingDefaultValueFields(
                  Set.of(IndexStateInfo.getDescriptor().findFieldByName("fields")))
              .print(currentStateInfo);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return JsonParser.parseString(stateString).getAsJsonObject();
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public double getNrtCachingDirectoryMaxMergeSizeMB() {
    return nrtCachingDirectoryMaxMergeSizeMB;
  }

  @Override
  public double getNrtCachingDirectoryMaxSizeMB() {
    return nrtCachingDirectoryMaxSizeMB;
  }

  @Override
  public double getMinRefreshSec() {
    return minRefreshSec;
  }

  @Override
  public double getMaxRefreshSec() {
    return maxRefreshSec;
  }

  @Override
  public double getMaxSearcherAgeSec() {
    return maxSearcherAgeSec;
  }

  @Override
  public double getIndexRamBufferSizeMB() {
    return indexRamBufferSizeMB;
  }

  @Override
  public int getAddDocumentsMaxBufferLen() {
    return addDocumentsMaxBufferLen;
  }

  @Override
  public int getSliceMaxDocs() {
    return sliceMaxDocs;
  }

  @Override
  public int getSliceMaxSegments() {
    return sliceMaxSegments;
  }

  @Override
  public int getVirtualShards() {
    if (!getGlobalState().getConfiguration().getVirtualSharding()) {
      return 1;
    }
    return virtualShards;
  }

  @Override
  public int getMaxMergedSegmentMB() {
    return maxMergedSegmentMB;
  }

  @Override
  public int getSegmentsPerTier() {
    return segmentsPerTier;
  }

  @Override
  public double getDefaultSearchTimeoutSec() {
    return defaultSearchTimeoutSec;
  }

  @Override
  public int getDefaultTerminateAfter() {
    return defaultTerminateAfter;
  }

  @Override
  public int getDefaultSearchTimeoutCheckEvery() {
    return defaultSearchTimeoutCheckEvery;
  }

  @Override
  public long getMaxMergePreCopyDurationSec() {
    return maxMergePreCopyDurationSec;
  }

  @Override
  public void addSuggest(String name, JsonObject o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Lookup> getSuggesters() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initWarmer(Archiver archiver) {
    initWarmer(archiver, uniqueName);
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<Integer, ShardState> entry : shards.entrySet()) {
      entry.getValue().close();
    }
  }

  static void validateSettings(IndexSettings settings) {
    if (settings.getNrtCachingDirectoryMaxSizeMB().getValue() < 0) {
      throw new IllegalArgumentException("nrtCachingDirectoryMaxSizeMB must be >= 0");
    }
    if (settings.getNrtCachingDirectoryMaxMergeSizeMB().getValue() < 0) {
      throw new IllegalArgumentException("nrtCachingDirectoryMaxMergeSizeMB must be >= 0");
    }
    int maxMergeCount = settings.getConcurrentMergeSchedulerMaxMergeCount().getValue();
    int maxThreadCount = settings.getConcurrentMergeSchedulerMaxThreadCount().getValue();
    if (maxMergeCount != ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS
        || maxThreadCount != ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS) {
      if (maxMergeCount == ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS
          || maxThreadCount == ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS) {
        throw new IllegalArgumentException(
            "both concurrentMergeSchedulerMaxMergeCount and concurrentMergeSchedulerMaxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS ("
                + ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS
                + ")");
      }
      if (maxThreadCount > maxMergeCount) {
        throw new IllegalArgumentException(
            "concurrentMergeSchedulerMaxThreadCount should be <= concurrentMergeSchedulerMaxMergeCount (= "
                + maxMergeCount
                + ")");
      }
    }
  }

  static void validateLiveSettings(IndexLiveSettings liveSettings) {
    if (liveSettings.getMaxRefreshSec().getValue() < liveSettings.getMinRefreshSec().getValue()) {
      throw new IllegalArgumentException("maxRefreshSec must be >= minRefreshSec");
    }
    if (liveSettings.getMaxSearcherAgeSec().getValue() < 0.0) {
      throw new IllegalArgumentException("maxSearcherAgeSec must be >= 0.0");
    }
    if (liveSettings.getIndexRamBufferSizeMB().getValue() <= 0.0) {
      throw new IllegalArgumentException("indexRamBufferSizeMB must be > 0.0");
    }
    if (liveSettings.getAddDocumentsMaxBufferLen().getValue() <= 0) {
      throw new IllegalArgumentException("addDocumentsMaxBufferLen must be > 0");
    }
    if (liveSettings.getSliceMaxDocs().getValue() <= 0) {
      throw new IllegalArgumentException("sliceMaxDocs must be > 0");
    }
    if (liveSettings.getSliceMaxSegments().getValue() <= 0) {
      throw new IllegalArgumentException("sliceMaxSegments must be > 0");
    }
    if (liveSettings.getVirtualShards().getValue() <= 0) {
      throw new IllegalArgumentException("virtualShards must be > 0");
    }
    if (liveSettings.getMaxMergedSegmentMB().getValue() < 0) {
      throw new IllegalArgumentException("maxMergedSegmentMB must be >= 0");
    }
    if (liveSettings.getSegmentsPerTier().getValue() < 2) {
      throw new IllegalArgumentException("segmentsPerTier must be >= 2");
    }
    if (liveSettings.getDefaultSearchTimeoutSec().getValue() < 0.0) {
      throw new IllegalArgumentException("defaultSearchTimeoutSec must be >= 0.0");
    }
    if (liveSettings.getDefaultSearchTimeoutCheckEvery().getValue() < 0) {
      throw new IllegalArgumentException("defaultSearchTimeoutCheckEvery must be >= 0");
    }
    if (liveSettings.getDefaultTerminateAfter().getValue() < 0) {
      throw new IllegalArgumentException("defaultTerminateAfter must be >= 0");
    }
    if (liveSettings.getMaxMergePreCopyDurationSec().getValue() < 0) {
      throw new IllegalArgumentException("maxMergePreCopyDurationSec must be >= 0");
    }
  }

  private static void validateIndexSort(Sort sort) {
    for (SortField sortField : sort.getSort()) {
      if (!ALLOWED_INDEX_SORT_TYPES.contains(sortField.getType())) {
        throw new IllegalArgumentException(
            "Sort field: "
                + sortField.getField()
                + ", type: "
                + sortField.getType()
                + " is not in allowed types: "
                + ALLOWED_INDEX_SORT_TYPES);
      }
    }
  }
}
