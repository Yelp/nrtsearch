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
package com.yelp.nrtsearch.server.index;

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
import com.yelp.nrtsearch.server.codec.ServerCodec;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.handler.SearchHandler.SearchHandlerException;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.search.sort.SortParser;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of index state which is immutable. */
public class ImmutableIndexState extends IndexState {
  private static final Logger logger = LoggerFactory.getLogger(ImmutableIndexState.class);

  public static final double DEFAULT_NRT_CACHING_MAX_MERGE_SIZE_MB = 5.0;
  public static final double DEFAULT_NRT_CACHING_MAX_SIZE_MB = 60.0;
  public static final boolean DEFAULT_MERGE_AUTO_THROTTLE = false;
  public static final String DEFAULT_DIRECTORY = "FSDirectory";
  public static final long DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS =
      500; // 500 milliseconds default

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
          .setMaxFullFlushMergeWaitMillis(
              UInt64Value.newBuilder().setValue(DEFAULT_MAX_FULL_FLUSH_MERGE_WAIT_MILLIS).build())
          .build();

  // Settings
  private final double nrtCachingDirectoryMaxMergeSizeMB;
  private final double nrtCachingDirectoryMaxSizeMB;
  private final int concurrentMergeSchedulerMaxThreadCount;
  private final int concurrentMergeSchedulerMaxMergeCount;
  private final Sort indexSort;
  private final boolean indexMergeSchedulerAutoThrottle;
  private final DirectoryFactory directoryFactory;
  private final long maxFullFlushMergeWaitMillis;

  public static final double DEFAULT_MAX_REFRESH_SEC = 1.0;
  public static final double DEFAULT_MIN_REFRESH_SEC = 0.05;
  public static final double DEFAULT_MAX_SEARCHER_AGE = 60.0;
  public static final double DEFAULT_INDEX_RAM_BUFFER_SIZE_MB = 16.0;
  public static final int DEFAULT_ADD_DOCS_MAX_BUFFER_LEN = 100;
  public static final int DEFAULT_SLICE_MAX_DOCS = 250_000;
  public static final int DEFAULT_SLICE_MAX_SEGMENTS = 5;
  public static final int DEFAULT_VIRTUAL_SHARDS = 1;
  public static final int DEFAULT_SEGMENTS_PER_TIER = 10;
  public static final int DEFAULT_DELETE_PCT_ALLOWED = 20;
  public static final int DEFAULT_MAX_MERGED_SEGMENT_MB = 5 * 1024;
  public static final int DEFAULT_PARALLEL_FETCH_CHUNK_SIZE = 50;

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
          .setDeletePctAllowed(Int32Value.newBuilder().setValue(DEFAULT_DELETE_PCT_ALLOWED).build())
          .setMaxMergedSegmentMB(
              Int32Value.newBuilder().setValue(DEFAULT_MAX_MERGED_SEGMENT_MB).build())
          // default unset
          .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(0).build())
          .setDefaultSearchTimeoutCheckEvery(Int32Value.newBuilder().setValue(0).build())
          .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(0).build())
          .setDefaultTerminateAfterMaxRecallCount(Int32Value.newBuilder().setValue(0).build())
          .setMaxMergePreCopyDurationSec(UInt64Value.newBuilder().setValue(0))
          .setVerboseMetrics(BoolValue.newBuilder().setValue(false).build())
          .setParallelFetchByField(BoolValue.newBuilder().setValue(false).build())
          .setParallelFetchChunkSize(
              Int32Value.newBuilder().setValue(DEFAULT_PARALLEL_FETCH_CHUNK_SIZE).build())
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
  private final int deletePctAllowed;
  private final double defaultSearchTimeoutSec;
  private final int defaultSearchTimeoutCheckEvery;
  private final int defaultTerminateAfter;
  private final int defaultTerminateAfterMaxRecallCount;
  private final long maxMergePreCopyDurationSec;
  private final boolean verboseMetrics;
  private final ParallelFetchConfig parallelFetchConfig;

  private final IndexStateManager indexStateManager;
  private final String uniqueName;
  private final IndexStateInfo currentStateInfo;
  private final IndexSettings mergedSettings;
  private final IndexLiveSettings mergedLiveSettings;
  private final IndexLiveSettings mergedLiveSettingsWithLocal;
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
   * @param liveSettingsOverrides local overrides for index live settings
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
      IndexLiveSettings liveSettingsOverrides,
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
    directoryFactory =
        DirectoryFactory.get(
            mergedSettings.getDirectory().getValue(), globalState.getConfiguration());
    maxFullFlushMergeWaitMillis = mergedSettings.getMaxFullFlushMergeWaitMillis().getValue();

    // live settings
    mergedLiveSettings =
        mergeLiveSettings(DEFAULT_INDEX_LIVE_SETTINGS, stateInfo.getLiveSettings());
    mergedLiveSettingsWithLocal = mergeLiveSettings(mergedLiveSettings, liveSettingsOverrides);

    validateLiveSettings(mergedLiveSettingsWithLocal);

    maxRefreshSec = mergedLiveSettingsWithLocal.getMaxRefreshSec().getValue();
    minRefreshSec = mergedLiveSettingsWithLocal.getMinRefreshSec().getValue();
    maxSearcherAgeSec = mergedLiveSettingsWithLocal.getMaxSearcherAgeSec().getValue();
    indexRamBufferSizeMB = mergedLiveSettingsWithLocal.getIndexRamBufferSizeMB().getValue();
    addDocumentsMaxBufferLen = mergedLiveSettingsWithLocal.getAddDocumentsMaxBufferLen().getValue();
    sliceMaxDocs = mergedLiveSettingsWithLocal.getSliceMaxDocs().getValue();
    sliceMaxSegments = mergedLiveSettingsWithLocal.getSliceMaxSegments().getValue();
    virtualShards = mergedLiveSettingsWithLocal.getVirtualShards().getValue();
    maxMergedSegmentMB = mergedLiveSettingsWithLocal.getMaxMergedSegmentMB().getValue();
    segmentsPerTier = mergedLiveSettingsWithLocal.getSegmentsPerTier().getValue();
    deletePctAllowed = mergedLiveSettingsWithLocal.getDeletePctAllowed().getValue();
    defaultSearchTimeoutSec = mergedLiveSettingsWithLocal.getDefaultSearchTimeoutSec().getValue();
    defaultSearchTimeoutCheckEvery =
        mergedLiveSettingsWithLocal.getDefaultSearchTimeoutCheckEvery().getValue();
    defaultTerminateAfter = mergedLiveSettingsWithLocal.getDefaultTerminateAfter().getValue();
    defaultTerminateAfterMaxRecallCount =
        mergedLiveSettingsWithLocal.getDefaultTerminateAfterMaxRecallCount().getValue();
    maxMergePreCopyDurationSec =
        mergedLiveSettingsWithLocal.getMaxMergePreCopyDurationSec().getValue();
    verboseMetrics = mergedLiveSettingsWithLocal.getVerboseMetrics().getValue();
    // Parallel fetch config
    int maxParallelism =
        globalState
            .getThreadPoolConfiguration()
            .getThreadPoolSettings(ExecutorFactory.ExecutorType.FETCH)
            .maxThreads();
    boolean parallelFetchByField = mergedLiveSettingsWithLocal.getParallelFetchByField().getValue();
    int parallelFetchChunkSize = mergedLiveSettingsWithLocal.getParallelFetchChunkSize().getValue();
    parallelFetchConfig =
        new ParallelFetchConfig(
            maxParallelism,
            parallelFetchByField,
            parallelFetchChunkSize,
            globalState.getFetchExecutor());

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
                      getSearchExecutor(),
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
  @Override
  public IndexStateInfo getIndexStateInfo() {
    return currentStateInfo;
  }

  /** Get the fully merged (with defaults) index settings. */
  public IndexSettings getMergedSettings() {
    return mergedSettings;
  }

  /**
   * Get the fully merged (with defaults) index live settings.
   *
   * @param withLocal If local overrides should be included in the live settings
   */
  public IndexLiveSettings getMergedLiveSettings(boolean withLocal) {
    return withLocal ? mergedLiveSettingsWithLocal : mergedLiveSettings;
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
      Mode serverMode,
      NrtDataManager nrtDataManager,
      long primaryGen,
      ReplicationServerClient primaryClient)
      throws IOException {
    if (isStarted()) {
      throw new IllegalStateException("Index \"" + getName() + "\" is already started");
    }

    // only create if the index has not been committed and there is no data to restore
    for (ShardState shard : shards.values()) {
      shard.setDoCreate(!currentStateInfo.getCommitted() && !nrtDataManager.hasRestoreData());
    }

    // start all local shards
    switch (serverMode) {
      case STANDALONE:
        for (ShardState shard : shards.values()) {
          shard.start(nrtDataManager);
        }
        break;
      case PRIMARY:
        for (ShardState shard : shards.values()) {
          shard.startPrimary(nrtDataManager, primaryGen);
        }
        break;
      case REPLICA:
        for (ShardState shard : shards.values()) {
          shard.startReplica(nrtDataManager, primaryClient, primaryGen);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown server mode: " + serverMode);
    }
  }

  @Override
  public FieldDef getField(String fieldName) {
    FieldDef fd = getMetaFields().get(fieldName);
    if (fd != null) {
      return fd;
    }
    return fieldAndFacetState.getFields().get(fieldName);
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
  public ParallelFetchConfig getParallelFetchConfig() {
    return parallelFetchConfig;
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
  public long commit() throws IOException {
    long gen = -1;
    for (ShardState shard : shards.values()) {
      gen = shard.commit();
    }
    return gen;
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
              MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
            MergeThread thread = super.getMergeThread(mergeSource, merge);
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

    // Set the maximum time in milliseconds to wait for merges when doing a full flush
    iwc.setMaxFullFlushMergeWaitMillis(maxFullFlushMergeWaitMillis);

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
  public int getDeletePctAllowed() {
    return deletePctAllowed;
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
  public int getDefaultTerminateAfterMaxRecallCount() {
    return defaultTerminateAfterMaxRecallCount;
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
  public boolean getVerboseMetrics() {
    return verboseMetrics;
  }

  @Override
  public long getMaxFullFlushMergeWaitMillis() {
    return maxFullFlushMergeWaitMillis;
  }

  @Override
  public void initWarmer(RemoteBackend remoteBackend) {
    initWarmer(remoteBackend, uniqueName);
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
    if (settings.getMaxFullFlushMergeWaitMillis().getValue() < 0) {
      throw new IllegalArgumentException("maxFullFlushMergeWaitMillis must be >= 0");
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
    if (liveSettings.getDeletePctAllowed().getValue() < 20
        || liveSettings.getDeletePctAllowed().getValue() > 50) {
      throw new IllegalArgumentException("deletePctAllowed must be between 20 and 50");
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
    if (liveSettings.getDefaultTerminateAfterMaxRecallCount().getValue() < 0) {
      throw new IllegalArgumentException("defaultTerminateAfterMaxRecallCount must be >= 0");
    }
    if (liveSettings.getMaxMergePreCopyDurationSec().getValue() < 0) {
      throw new IllegalArgumentException("maxMergePreCopyDurationSec must be >= 0");
    }
    if (liveSettings.getParallelFetchChunkSize().getValue() <= 0) {
      throw new IllegalArgumentException("parallelFetchChunkSize must be > 0");
    }
  }

  private static void validateIndexSort(Sort sort) {
    for (SortField sortField : sort.getSort()) {
      if (sortField.getIndexSorter() == null) {
        throw new IllegalArgumentException(
            "Sort field: \"" + sortField.getField() + "\" does not support index sorting");
      }
    }
  }
}
