/*
 * Copyright 2020 Yelp Inc.
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.SettingsRequest;
import com.yelp.nrtsearch.server.grpc.SnapshotId;
import com.yelp.nrtsearch.server.luceneserver.BuildSuggestHandler;
import com.yelp.nrtsearch.server.luceneserver.CreateSnapshotHandler;
import com.yelp.nrtsearch.server.luceneserver.DirectoryFactory;
import com.yelp.nrtsearch.server.luceneserver.GenFileUtil;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.LiveSettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.RegisterFieldsHandler;
import com.yelp.nrtsearch.server.luceneserver.Restorable;
import com.yelp.nrtsearch.server.luceneserver.SaveState;
import com.yelp.nrtsearch.server.luceneserver.ServerCodec;
import com.yelp.nrtsearch.server.luceneserver.SettingsHandler;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefBindings;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.packed.PackedInts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds all state associated with one index. On startup and on creating a new index, the index
 * loads its state but does not start itself. At this point, settings can be changed, and then the
 * index must be {@link #}ed before it can be used for indexing and searching, at which point only
 * live settings may be changed.
 *
 * <p>Filesystem state: each index has its own rootDir, specified when the index is created. Under
 * each ootDir:
 *
 * <ul>
 *   <li>{@code index} has the Lucene index
 *   <li>{@code taxonomy} has the taxonomy index (empty if no facets are indexed)
 *   <li>{@code state} has all current settings
 *   <li>{@code state/state.N} gen files holds all settings
 *   <li>{@code state/saveLoadRefCounts.N} gen files holds all reference counts from live snapshots
 * </ul>
 */
public class LegacyIndexState extends IndexState implements Restorable {
  private static final Logger logger = LoggerFactory.getLogger(LegacyIndexState.class);
  /** Which norms format to use for all indexed fields. */
  private String normsFormat = "Lucene80";

  /** If normsFormat is Lucene80, what acceptableOverheadRatio to pass. */
  private float normsAcceptableOverheadRatio = PackedInts.FASTEST;

  /** Creates directories */
  private DirectoryFactory df = DirectoryFactory.get("MMapDirectory");

  private IdFieldDef idFieldDef = null;

  /** Optional index time sorting (write once!) or null if no index time sorting */
  private Sort indexSort;

  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  /** Holds pending save state, written to state.N file on commit. */
  private final SaveState saveState = new SaveState();

  @Override
  public ShardState addShard(int shardOrd, boolean doCreate) {
    if (shards.containsKey(shardOrd)) {
      throw new IllegalArgumentException(
          "shardOrd=" + shardOrd + " already exists in index + \"" + getName() + "\"");
    }
    ShardState shard =
        new ShardState(
            new LegacyStateManager(this),
            getName(),
            getRootDir(),
            getSearchThreadPoolExecutor(),
            shardOrd,
            doCreate);
    // nocommit fail if there is already a shard here?
    shards.put(shardOrd, shard);
    return shard;
  }

  @Override
  public void deleteIndex() throws IOException {
    for (ShardState shardState : shards.values()) {
      shardState.deleteShard();
    }
    deleteIndexRootDir();
    getGlobalState().deleteIndex(getName());
  }

  /** True if this index has at least one commit. */
  @Override
  public boolean hasCommit() throws IOException {
    return saveLoadState.getNextWriteGen() != 0;
  }

  /** Record that this snapshot id refers to the current generation, returning it. */
  @Override
  public synchronized long incRefLastCommitGen() throws IOException {
    long nextGen = saveLoadState.getNextWriteGen();
    if (nextGen == 0) {
      throw new IllegalStateException("no commit exists");
    }
    long result = nextGen - 1;
    incRef(result);
    return result;
  }

  private synchronized void incRef(long stateGen) throws IOException {
    Integer rc = genRefCounts.get(stateGen);
    if (rc == null) {
      genRefCounts.put(stateGen, 1);
    } else {
      genRefCounts.put(stateGen, 1 + rc);
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  /** Drop this snapshot from the references. */
  @Override
  public synchronized void decRef(long stateGen) throws IOException {
    Integer rc = genRefCounts.get(stateGen);
    if (rc == null) {
      throw new IllegalArgumentException("stateGen=" + stateGen + " is not held by a snapshot");
    }
    assert rc > 0;
    if (rc == 1) {
      genRefCounts.remove(stateGen);
    } else {
      genRefCounts.put(stateGen, rc - 1);
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  /** Tracks snapshot references to generations. */
  private static class SaveLoadRefCounts extends GenFileUtil<Map<Long, Integer>> {
    private final JsonParser jsonParser = new JsonParser();

    public SaveLoadRefCounts(Directory dir) {
      super(dir, "stateRefCounts");
    }

    @Override
    protected void saveOne(IndexOutput out, Map<Long, Integer> refCounts) throws IOException {
      JsonObject o = new JsonObject();
      for (Map.Entry<Long, Integer> ent : refCounts.entrySet()) {
        o.addProperty(ent.getKey().toString(), ent.getValue());
      }
      byte[] bytes = LegacyIndexState.toUTF8(o.toString());
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected Map<Long, Integer> loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, bytes.length);
      String s = LegacyIndexState.fromUTF8(bytes);
      JsonObject o;
      try {
        o = jsonParser.parse(s).getAsJsonObject();
      } catch (JsonParseException pe) {
        // Change to IOExc so gen logic will fallback:
        throw new IOException("invalid JSON when parsing refCounts", pe);
      }
      Map<Long, Integer> refCounts = new HashMap<>();
      for (Map.Entry<String, JsonElement> ent : o.entrySet()) {
        refCounts.put(Long.parseLong(ent.getKey()), ent.getValue().getAsInt());
      }
      return refCounts;
    }
  }

  private class SaveLoadState extends GenFileUtil<JsonObject> {
    private final JsonParser jsonParser = new JsonParser();

    public SaveLoadState(Directory dir) {
      super(dir, "state");
    }

    @Override
    protected void saveOne(IndexOutput out, JsonObject state) throws IOException {
      // Pretty print:
      String pretty = gson.toJson(state);
      byte[] bytes = LegacyIndexState.toUTF8(pretty);
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected JsonObject loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      String s = LegacyIndexState.fromUTF8(bytes);
      JsonObject ret;
      try {
        ret = jsonParser.parse(s).getAsJsonObject();
      } catch (JsonParseException pe) {
        // Change to IOExc so gen logic will fallback:
        throw new IOException("invalid JSON when parsing refCounts", pe);
      }
      return ret;
    }

    @Override
    protected boolean canDelete(long gen) {
      return !hasRef(gen);
    }
  }

  /** Which snapshots (List&lt;Long&gt;) are referencing which save state generations. */
  Map<Long, Integer> genRefCounts;

  SaveLoadRefCounts saveLoadGenRefCounts;

  /** Holds all settings, field definitions */
  SaveLoadState saveLoadState;

  /**
   * When a search is waiting on a specific generation, we will wait at most this many seconds
   * before reopening (default 50 msec).
   */
  volatile double minRefreshSec = .05f;

  /**
   * When no searcher is waiting on a specific generation, we will wait at most this many seconds
   * before proactively reopening (default 1 sec).
   */
  volatile double maxRefreshSec = 1.0f;

  /**
   * Once a searcher becomes stale (i.e., a new searcher is opened), we will close it after this
   * much time (default: 60 seconds). If this is too small, it means that old searches returning for
   * a follow-on query may find their searcher pruned (lease expired).
   */
  volatile double maxSearcherAgeSec = 60;

  /** RAM buffer size passed to {@link IndexWriterConfig#setRAMBufferSizeMB}. */
  volatile double indexRamBufferSizeMB = 16;

  /** Max number of documents to be added at a time. */
  int addDocumentsMaxBufferLen = 100;

  /** Max documents allowed in a parallel search slice */
  volatile int sliceMaxDocs = DEFAULT_SLICE_MAX_DOCS;
  /** Max segments allowed in a parallel search slice */
  volatile int sliceMaxSegments = DEFAULT_SLICE_MAX_SEGMENTS;

  /** Number of virtual shards to use for merges and parallel search */
  volatile int virtualShards = 1;

  /** Max segment size after merge */
  volatile int maxMergedSegmentMB = 0;

  /** Segments per tier used by {@link TieredMergePolicy} */
  volatile int segmentsPerTier = 0;

  /** Default search timeout, when not specified in the request */
  volatile double defaultSearchTimeoutSec = 0;

  /** Default search timeout check every, when not specified in the request */
  volatile int defaultSearchTimeoutCheckEvery = 0;

  /** Default terminate after, when not specified in the request */
  volatile int defaultTerminateAfter = 0;

  /** True if this is a new index. */
  private final boolean doCreate;

  /**
   * Sole constructor; creates a new index or loads an existing one if it exists, but does not start
   * the index.
   */
  public LegacyIndexState(
      GlobalState globalState, String name, Path rootDir, boolean doCreate, boolean hasRestore)
      throws IOException {
    super(globalState, name, rootDir);
    this.doCreate = doCreate;

    if (doCreate == false && !hasRestore) {
      initSaveLoadState();
    }
  }

  public void initSaveLoadState() throws IOException {
    Path stateDirFile;
    if (getRootDir() != null) {
      stateDirFile = getStateDirectoryPath();
      // if (!stateDirFile.exists()) {
      // stateDirFile.mkdirs();
      // }
    } else {
      stateDirFile = null;
    }

    // nocommit who closes this?
    // nocommit can't this be in the rootDir directly?
    Directory stateDir = df.open(stateDirFile, IndexPreloadConfig.PRELOAD_ALL);

    saveLoadGenRefCounts = new SaveLoadRefCounts(stateDir);

    // Snapshot ref counts:
    genRefCounts = saveLoadGenRefCounts.load();
    if (genRefCounts == null) {
      genRefCounts = new HashMap<>();
    }

    saveLoadState = new SaveLoadState(stateDir);

    JsonObject priorState = saveLoadState.load();
    if (priorState != null) {
      load(priorState.getAsJsonObject("state"));
    }
  }

  /** Load all previously saved state. */
  public synchronized void load(JsonObject jsonObject) throws IOException {

    // To load, we invoke each handler from the save state,
    // as if the app had just done so from a fresh index,
    // except for suggesters which uses a dedicated load
    // method:
    // registerFieldsHandler.handle(...)
    // settingsHandler.handle(...)
    // liveSettingsHandler.handle(...)

    // Do fields first, because indexSort could reference fields:
    // Field defs:
    JsonElement fieldsState = jsonObject.get("fields");
    JsonObject top = new JsonObject();
    top.add("fields", fieldsState);

    RegisterFieldsHandler registerFieldsHandler = new RegisterFieldsHandler();
    String jsonFields = convertFieldStateToJsonFieldDefRequest(fieldsState);
    FieldDefRequest jsonFieldDefRequest = getFieldDefRequest(jsonFields);
    try {
      registerFieldsHandler.handle(this, jsonFieldDefRequest);
    } catch (RegisterFieldsHandler.RegisterFieldsException e) {
      logger.warn("Reinstating state for registered fields failed", e);
      throw new RuntimeException(e);
    }

    JsonElement settingsState = jsonObject.get("settings");
    SettingsRequest settingsRequest =
        buildSettingsRequest(settingsState == null ? "" : settingsState.toString());
    SettingsHandler settingsHander = new SettingsHandler();
    try {
      settingsHander.handle(this, settingsRequest);
    } catch (SettingsHandler.SettingsHandlerException e) {
      logger.warn("Reinstating state for settings failed", e);
      throw new RuntimeException(e);
    }

    JsonElement liveSettingsState = jsonObject.get("liveSettings");
    LiveSettingsRequest liveSettingsRequest =
        buildLiveSettingsRequest(liveSettingsState == null ? "" : liveSettingsState.toString());
    LiveSettingsHandler liveSettingsHandler = new LiveSettingsHandler();
    liveSettingsHandler.handle(this, liveSettingsRequest);

    // do not init suggesters here: they can take non-trivial heap, and they need Directory to be
    // created
    suggesterSettings = (JsonObject) jsonObject.get("suggest");
  }

  /** The field definitions (registerField) */
  private final Map<String, FieldDef> fields = new ConcurrentHashMap<>();

  /** Contains fields set as facetIndexFieldName. */
  private final Set<String> internalFacetFieldNames =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final FacetsConfig facetsConfig = new FacetsConfig();

  /**
   * Fields using facets with global ordinals that should be loaded up front with each new reader
   */
  private final Map<String, FieldDef> eagerGlobalOrdinalFields = new ConcurrentHashMap<>();

  /** {@link Bindings} to pass when evaluating expressions. */
  private final Bindings exprBindings = new FieldDefBindings(fields);

  private final Map<Integer, ShardState> shards = new ConcurrentHashMap<>();

  /** Built suggest implementations */
  private final Map<String, Lookup> suggesters = new ConcurrentHashMap<>();

  /** Holds suggest settings loaded but not yet started */
  private JsonObject suggesterSettings;

  @Override
  public void close() throws IOException {
    logger.info(String.format("IndexState.close name= %s", getName()));
    List<Closeable> closeables = new ArrayList<>();
    closeables.addAll(shards.values());
    closeables.addAll(fields.values());
    for (Lookup suggester : suggesters.values()) {
      if (suggester instanceof Closeable) {
        closeables.add((Closeable) suggester);
      }
    }
    IOUtils.close(closeables);

    // nocommit should we remove this instance?  if app
    // starts again ... should we re-use the current
    // instance?  seems ... risky?
    // nocommit this is dangerous .. eg Server iterates
    // all IS.indices and closes ...:
    // nocommit need sync:

    super.close();
  }

  /** True if this generation is still referenced by at least one snapshot. */
  public synchronized boolean hasRef(long gen) {
    Integer rc = genRefCounts.get(gen);
    if (rc == null) {
      return false;
    } else {
      assert rc > 0;
      return true;
    }
  }

  @Override
  public Map<Long, Integer> getGenRefCounts() {
    return genRefCounts;
  }

  /**
   * Commit all state and shards. If backupFromIncArchiver is passed in it will also attempt to use
   * IndexArchiver to upload files to remote storage
   */
  @Override
  public synchronized long commit(boolean backupFromIncArchiver) throws IOException {

    if (saveLoadState == null) {
      initSaveLoadState();
    }

    // nocommit this does nothing on replica?  make a failing test!
    long gen = -1;
    for (ShardState shard : shards.values()) {
      gen = shard.commit();
    }

    for (Lookup suggester : suggesters.values()) {
      if (suggester instanceof AnalyzingInfixSuggester) {
        ((AnalyzingInfixSuggester) suggester).commit();
      }
    }

    // nocommit needs test case that creates index, changes
    // some settings, closes it w/o ever starting it:
    // settings changes are lost then?

    JsonObject saveState = new JsonObject();
    saveState.add("state", getSaveState());
    saveLoadState.save(saveState);
    SnapshotId snapshotId = null;
    try {
      if (this.getShard(0).isPrimary()
          && getGlobalState().getIncArchiver().isPresent()
          && backupFromIncArchiver) {
        CreateSnapshotRequest createSnapshotRequest =
            CreateSnapshotRequest.newBuilder().setIndexName(getName()).build();

        snapshotId =
            new CreateSnapshotHandler().createSnapshot(this, createSnapshotRequest).getSnapshotId();
        // upload data
        Collection<String> segmentFiles = getSegmentFilesInSnapshot(this, snapshotId);
        String resourceData = IndexBackupUtils.getResourceData(getName());
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
        // upload metadata
        String resourceMetadata = IndexBackupUtils.getResourceMetadata(getName());
        versionHash =
            incArchiver.upload(
                getGlobalState().getConfiguration().getServiceName(),
                resourceMetadata,
                getGlobalState().getStateDir(),
                Collections.emptyList(),
                Collections.emptyList(),
                true);
        incArchiver.blessVersion(
            getGlobalState().getConfiguration().getServiceName(), resourceMetadata, versionHash);
      }
    } finally {
      if (snapshotId != null) {
        releaseSnapshot(this, getName(), snapshotId);
      }
    }

    return gen;
  }

  /** Get the current save state. */
  public JsonObject getSaveState() throws IOException {
    return saveState.getSaveState();
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return df;
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
  public double getNrtCachingDirectoryMaxMergeSizeMB() {
    return getDoubleSetting("nrtCachingDirectoryMaxMergeSizeMB", 5.0);
  }

  @Override
  public double getNrtCachingDirectoryMaxSizeMB() {
    return getDoubleSetting("nrtCachingDirectoryMaxSizeMB", 60.0);
  }

  /**
   * Retrieve the field's type.
   *
   * @throws IllegalArgumentException if the field was not registered.
   */
  @Override
  public FieldDef getField(String fieldName) {
    FieldDef fd = getMetaFields().get(fieldName);
    if (fd != null) {
      return fd;
    }
    fd = fields.get(fieldName);
    if (fd == null) {
      String message =
          "field \"" + fieldName + "\" is unknown: it was not registered with registerField";
      throw new IllegalArgumentException(message);
    }
    return fd;
  }

  /** Find the most recent generation in the directory for this prefix. */
  public static long getLastGen(Path dir, String prefix) throws IOException {
    assert isSimpleName(prefix);
    prefix += '.';
    long lastGen = -1;
    if (Files.exists(dir) && Files.isDirectory(dir)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
        for (Path subFile : stream) {
          String name = subFile.getFileName().toString();
          if (name.startsWith(prefix)) {
            lastGen = Math.max(lastGen, Long.parseLong(name.substring(prefix.length())));
          }
        }
      }
    }

    return lastGen;
  }

  /** Verifies if it has nested child object fields. */
  @Override
  public synchronized boolean hasNestedChildFields() {
    for (FieldDef fieldDef : fields.values()) {
      if (fieldDef instanceof ObjectFieldDef && ((ObjectFieldDef) fieldDef).isNestedDoc()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Live setting: set the minimum and maximum refresh time (seconds), which is the longest amount
   * of time a client may wait for a searcher to reopen.
   */
  public synchronized void setRefreshSec(double min, double max) {
    if (min <= 0.0 || max <= 0.0) {
      throw new IllegalArgumentException("Min and Max refresh seconds must be > 0");
    }
    if (max < min) {
      throw new IllegalArgumentException("Max refresh seconds must be >= Min refresh seconds");
    }
    minRefreshSec = min;
    maxRefreshSec = max;
    saveState.getLiveSettings().addProperty("minRefreshSec", min);
    saveState.getLiveSettings().addProperty("maxRefreshSec", max);
    for (ShardState shardState : shards.values()) {
      shardState.restartReopenThread();
    }
  }

  /** Live setting: once a searcher becomes stale, we will close it after this many seconds. */
  public synchronized void setMaxSearcherAgeSec(double d) {
    maxSearcherAgeSec = d;
    saveState.getLiveSettings().addProperty("maxSearcherAgeSec", d);
  }

  /**
   * Live setting: how much RAM to use for buffered documents during indexing (passed to {@link
   * IndexWriterConfig#setRAMBufferSizeMB}.
   */
  public synchronized void setIndexRamBufferSizeMB(double d) {
    indexRamBufferSizeMB = d;
    saveState.getLiveSettings().addProperty("indexRamBufferSizeMB", d);

    // nocommit sync: what if closeIndex is happening in
    // another thread:
    for (ShardState shard : shards.values()) {
      if (shard.writer != null) {
        // Propagate the change to the open IndexWriter
        shard.writer.getConfig().setRAMBufferSizeMB(d);
      } else if (shard.nrtPrimaryNode != null) {
        shard.nrtPrimaryNode.setRAMBufferSizeMB(d);
      }
    }
  }

  /** Live setting: max number of documents to add at a time. */
  public synchronized void setAddDocumentsMaxBufferLen(int i) {
    addDocumentsMaxBufferLen = i;
    saveState.getLiveSettings().addProperty("addDocumentsMaxBufferLen", i);
  }

  /** Live setting: max number of documents to add at a time. */
  @Override
  public int getAddDocumentsMaxBufferLen() {
    return addDocumentsMaxBufferLen;
  }

  /**
   * Set the maximum number of document in each parallel search slice.
   *
   * @param docs maximum slice documents
   * @throws IllegalArgumentException if docs <= 0
   */
  public synchronized void setSliceMaxDocs(int docs) {
    if (docs <= 0) {
      throw new IllegalArgumentException("Max slice docs must be greater than 0.");
    }
    sliceMaxDocs = docs;
    saveState.getLiveSettings().addProperty("sliceMaxDocs", docs);
  }

  /** Get the maximum docs per parallel search slice. */
  @Override
  public int getSliceMaxDocs() {
    return sliceMaxDocs;
  }

  /**
   * Set the maximum number of segments in each parallel search slice.
   *
   * @param segments maximum slice segments
   * @throws IllegalArgumentException if segments <= 0
   */
  public synchronized void setSliceMaxSegments(int segments) {
    if (segments <= 0) {
      throw new IllegalArgumentException("Max slice segments must be greater than 0.");
    }
    sliceMaxSegments = segments;
    saveState.getLiveSettings().addProperty("sliceMaxSegments", segments);
  }

  /** Get the maximum segments per parallel search slice. */
  @Override
  public int getSliceMaxSegments() {
    return sliceMaxSegments;
  }

  /**
   * Set the number of virtual shards to use for this index.
   *
   * @param shards number of virtual shards to use
   * @throws IllegalArgumentException if shards <= 0
   */
  public synchronized void setVirtualShards(int shards) {
    if (shards <= 0) {
      throw new IllegalArgumentException("Number of virtual shards must be greater than 0.");
    }

    if (!getGlobalState().getConfiguration().getVirtualSharding() && shards > 1) {
      logger.warn(
          String.format("Setting virtual shards to %d, but virtual sharding is disabled.", shards));
    }

    virtualShards = shards;
    saveState.getLiveSettings().addProperty("virtualShards", shards);
  }

  /**
   * Get the number of virtual shards for this index. If virtual sharding is disabled, this always
   * returns 1.
   */
  @Override
  public int getVirtualShards() {
    if (!getGlobalState().getConfiguration().getVirtualSharding()) {
      return 1;
    }
    return virtualShards;
  }

  /** Set maximum sized segment to produce during normal merging */
  public synchronized void setMaxMergedSegmentMB(int maxMergedSegmentMB) {
    if (maxMergedSegmentMB <= 0) {
      throw new IllegalArgumentException("Max merged segment size must be greater than 0.");
    }
    this.maxMergedSegmentMB = maxMergedSegmentMB;
    saveState.getLiveSettings().addProperty("maxMergedSegmentMB", maxMergedSegmentMB);
  }

  /** Get maximum sized segment to produce during normal merging */
  @Override
  public int getMaxMergedSegmentMB() {
    return maxMergedSegmentMB;
  }

  /**
   * Set segments per tier used by {@link TieredMergePolicy}.
   *
   * @param segmentsPerTier segments per tier
   * @throws IllegalArgumentException if segmentsPerTier < 2
   */
  public synchronized void setSegmentsPerTier(int segmentsPerTier) {
    if (segmentsPerTier < 2) {
      throw new IllegalArgumentException("Segments per tier must be >= 2.");
    }
    this.segmentsPerTier = segmentsPerTier;
    saveState.getLiveSettings().addProperty("segmentsPerTier", segmentsPerTier);
  }

  /** Get the number of segments per tier used by merge policy, or 0 if using policy default. */
  @Override
  public int getSegmentsPerTier() {
    return segmentsPerTier;
  }

  /**
   * Set the default search timeout.
   *
   * @param defaultSearchTimeoutSec default timeout
   * @throws IllegalArgumentException if value is < 0
   */
  public synchronized void setDefaultSearchTimeoutSec(double defaultSearchTimeoutSec) {
    if (defaultSearchTimeoutSec < 0) {
      throw new IllegalArgumentException("Default search timeout must be >= 0.");
    }
    this.defaultSearchTimeoutSec = defaultSearchTimeoutSec;
    saveState.getLiveSettings().addProperty("defaultSearchTimeoutSec", defaultSearchTimeoutSec);
  }

  /** Get the default search timeout. */
  @Override
  public double getDefaultSearchTimeoutSec() {
    return defaultSearchTimeoutSec;
  }

  /**
   * Set the default search timeout check every.
   *
   * @param defaultSearchTimeoutCheckEvery default search timeout check every
   * @throws IllegalArgumentException if value is < 0
   */
  public synchronized void setDefaultSearchTimeoutCheckEvery(int defaultSearchTimeoutCheckEvery) {
    if (defaultSearchTimeoutCheckEvery < 0) {
      throw new IllegalArgumentException("Default search timeout check every must be >= 0.");
    }
    this.defaultSearchTimeoutCheckEvery = defaultSearchTimeoutCheckEvery;
    saveState
        .getLiveSettings()
        .addProperty("defaultSearchTimeoutCheckEvery", defaultSearchTimeoutCheckEvery);
  }

  /** Get the default terminate after. */
  @Override
  public int getDefaultTerminateAfter() {
    return defaultTerminateAfter;
  }

  /**
   * Set the default terminate after.
   *
   * @param defaultTerminateAfter default terminate after
   * @throws IllegalArgumentException if value is < 0
   */
  public synchronized void setDefaultTerminateAfter(int defaultTerminateAfter) {
    if (defaultTerminateAfter < 0) {
      throw new IllegalArgumentException("Default terminate after must be >= 0.");
    }
    this.defaultTerminateAfter = defaultTerminateAfter;
    saveState.getLiveSettings().addProperty("defaultTerminateAfter", defaultTerminateAfter);
  }

  /** Get the default search timeout check every. */
  @Override
  public int getDefaultSearchTimeoutCheckEvery() {
    return defaultSearchTimeoutCheckEvery;
  }

  /** Returns JSON representation of all live settings. */
  public String getLiveSettingsJSON() {
    return saveState.getLiveSettings().toString();
  }

  @Override
  public boolean hasFacets() {
    return !internalFacetFieldNames.isEmpty();
  }

  @Override
  public Set<String> getInternalFacetFieldNames() {
    return internalFacetFieldNames;
  }

  @Override
  public FacetsConfig getFacetsConfig() {
    return facetsConfig;
  }

  /** Returns JSON representation of all registered fields. */
  @Override
  public synchronized String getAllFieldsJSON() {
    return saveState.getFields().toString();
  }

  /** Returns JSON representation of all settings. */
  public synchronized String getSettingsJSON() {
    return saveState.getSettings().toString();
  }

  @Override
  public Map<String, FieldDef> getAllFields() {
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Optional<IdFieldDef> getIdFieldDef() {
    return Optional.ofNullable(idFieldDef);
  }

  /** Records a new field in the internal {@code fields} state. */
  @Override
  public synchronized void addField(FieldDef fd, JsonObject jsonObject) {
    if (fields.containsKey(fd.getName())) {
      throw new IllegalArgumentException("field \"" + fd.getName() + "\" was already registered");
    }
    if (getMetaFields().containsKey(fd.getName())) {
      throw new IllegalArgumentException(
          "field \"" + fd.getName() + "\" is a predefined meta field");
    }
    // only json for top level fields needs to be added to the save state
    if (!isChildName(fd.getName())) {
      if (jsonObject == null) {
        throw new IllegalArgumentException("Field json cannot be null for " + fd.getName());
      }
      assert null == saveState.getFields().get(fd.getName());
      saveState.getFields().add(fd.getName(), jsonObject);
    } else if (jsonObject != null) {
      throw new IllegalArgumentException(
          "Field json should not be specified for child field " + fd.getName());
    }

    fields.put(fd.getName(), fd);
    // nocommit support sorted set dv facets
    if (fd instanceof IndexableFieldDef) {
      IndexableFieldDef.FacetValueType facetValueType = fd.getFacetValueType();
      if (facetValueType != IndexableFieldDef.FacetValueType.NO_FACETS
          && facetValueType != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
        internalFacetFieldNames.add(facetsConfig.getDimConfig(fd.getName()).indexFieldName);
      }
    }
    // register fields that need global ordinals created up front
    if (fd.getEagerGlobalOrdinals()) {
      eagerGlobalOrdinalFields.put(fd.getName(), fd);
    }
    if (fd instanceof IdFieldDef) {
      idFieldDef = (IdFieldDef) fd;
    }
  }

  public void setNormsFormat(String format, float acceptableOverheadRatio) {
    this.normsFormat = format;
    // nocommit not used anymore?
    this.normsAcceptableOverheadRatio = acceptableOverheadRatio;
  }

  /** Record the {@link DirectoryFactory} to use for this index. */
  public synchronized void setDirectoryFactory(DirectoryFactory df, String directoryClassName) {
    if (isStarted()) {
      throw new IllegalStateException(
          "index \"" + getName() + "\": cannot change Directory when the index is running");
    }
    saveState.getSettings().addProperty("directory", directoryClassName);
    this.df = df;
  }

  public void setIndexSort(Sort sort, JsonObject saveState) {
    if (isStarted()) {
      throw new IllegalStateException(
          "index \"" + getName() + "\": cannot change index sort when the index is running");
    }
    if (this.indexSort != null && this.indexSort.equals(sort) == false) {
      throw new IllegalStateException("index \"" + getName() + "\": cannot change index sort");
    }
    this.saveState.getSettings().add("indexSort", saveState);
    this.indexSort = sort;
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

  /** Fold in new non-live settings from the incoming request into the stored settings. */
  public synchronized void mergeSimpleSettings(SettingsRequest settingsRequest) {
    if (isStarted()) {
      throw new IllegalStateException(
          "index \"" + getName() + "\" was already started (cannot change non-live settings)");
    }
    saveState.getSettings().addProperty("mergeMaxMBPerSec", settingsRequest.getMergeMaxMBPerSec());
    saveState
        .getSettings()
        .addProperty(
            "nrtCachingDirectoryMaxMergeSizeMB",
            settingsRequest.getNrtCachingDirectoryMaxMergeSizeMB());
    saveState
        .getSettings()
        .addProperty(
            "nrtCachingDirectoryMaxSizeMB", settingsRequest.getNrtCachingDirectoryMaxSizeMB());
    if ((settingsRequest.getConcurrentMergeSchedulerMaxThreadCount() != 0)
        && settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
      saveState
          .getSettings()
          .addProperty(
              "concurrentMergeSchedulerMaxThreadCount",
              settingsRequest.getConcurrentMergeSchedulerMaxThreadCount());
      saveState
          .getSettings()
          .addProperty(
              "concurrentMergeSchedulerMaxMergeCount",
              settingsRequest.getConcurrentMergeSchedulerMaxMergeCount());
    }
    saveState
        .getSettings()
        .addProperty(
            "indexMergeSchedulerAutoThrottle",
            settingsRequest.getIndexMergeSchedulerAutoThrottle());
  }

  @Override
  public void start(
      Mode serverMode, Path dataPath, long primaryGen, ReplicationServerClient primaryClient)
      throws IOException {
    if (shards.size() == 0) {
      throw new IllegalStateException("No shards to start for index: " + getName());
    }
    if (isStarted()) {
      throw new IllegalStateException("index \"" + getName() + "\" was already started");
    }

    if (!doCreate && dataPath != null) {
      if (getRootDir() != null) {
        // copy downloaded data into rootDir
        restoreDir(dataPath, getRootDir());
        initSaveLoadState();
      }
    }

    // ensure state is loaded
    if (saveLoadState == null) {
      initSaveLoadState();
    }

    // start all local shards
    switch (serverMode) {
      case STANDALONE:
        for (ShardState shard : shards.values()) {
          shard.start();
        }
        if (suggesterSettings != null) {
          // load suggesters:
          new BuildSuggestHandler(getSearchThreadPoolExecutor()).load(this, suggesterSettings);
          suggesterSettings = null;
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

  @Override
  public IndexWriterConfig getIndexWriterConfig(
      IndexWriterConfig.OpenMode openMode, Directory origIndexDir, int shardOrd)
      throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer);
    iwc.setOpenMode(openMode);
    if (getGlobalState().getConfiguration().getIndexVerbose()) {
      logger.info("Enabling verbose logging for Lucene NRT");
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    }

    if (indexSort != null) {
      iwc.setIndexSort(indexSort);
    }

    iwc.setSimilarity(sim);
    iwc.setRAMBufferSizeMB(indexRamBufferSizeMB);

    // nocommit in primary case we can't do this?
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

    if (getBooleanSetting("indexMergeSchedulerAutoThrottle", false)) {
      cms.enableAutoIOThrottle();
    } else {
      cms.disableAutoIOThrottle();
    }

    if (hasSetting("concurrentMergeSchedulerMaxMergeCount")) {
      // SettingsHandler verifies this:
      assert hasSetting("concurrentMergeSchedulerMaxThreadCount");
      cms.setMaxMergesAndThreads(
          getIntSetting("concurrentMergeSchedulerMaxMergeCount"),
          getIntSetting("concurrentMergeSchedulerMaxThreadCount"));
    }

    iwc.setIndexDeletionPolicy(
        new PersistentSnapshotDeletionPolicy(
            new KeepOnlyLastCommitDeletionPolicy(),
            origIndexDir,
            IndexWriterConfig.OpenMode.CREATE_OR_APPEND));

    iwc.setCodec(new ServerCodec(new LegacyStateManager(this)));

    TieredMergePolicy mergePolicy;
    if (getGlobalState().getConfiguration().getVirtualSharding()) {
      mergePolicy = new BucketedTieredMergePolicy(this::getVirtualShards);
    } else {
      mergePolicy = new TieredMergePolicy();
    }
    if (maxMergedSegmentMB > 0) {
      mergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
    }
    if (segmentsPerTier > 1) {
      mergePolicy.setSegmentsPerTier(segmentsPerTier);
    }
    iwc.setMergePolicy(mergePolicy);

    return iwc;
  }

  boolean getBooleanSetting(String name, boolean val) {
    JsonElement settingValue = saveState.getSettings().get(name);
    return settingValue == null ? val : settingValue.getAsBoolean();
  }

  double getDoubleSetting(String name, double val) {
    JsonElement settingValue = saveState.getSettings().get(name);
    return settingValue == null ? val : settingValue.getAsDouble();
  }

  int getIntSetting(String name) {
    return saveState.getSettings().get(name).getAsInt();
  }

  boolean hasSetting(String name) {
    return saveState.getSettings().get(name) != null;
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

  // Convert valid JSON string for FieldDefRequest (see registerFields API) to its protobuff
  // counterpart
  private FieldDefRequest getFieldDefRequest(String jsonStr) {
    logger.info(String.format("Converting fields %s to proto FieldDefRequest", jsonStr));
    FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    FieldDefRequest fieldDefRequest = fieldDefRequestBuilder.build();
    logger.info(String.format("jsonStr converted to proto FieldDefRequest %s", fieldDefRequest));
    return fieldDefRequest;
  }

  // Builds the valid Json format for FieldDefRequest from fieldsState json
  private String convertFieldStateToJsonFieldDefRequest(JsonElement fieldsState) {
    JsonObject newObj = new JsonObject();
    // set indexName
    newObj.addProperty("indexName", getName());
    JsonArray jsonFields = new JsonArray();
    newObj.add("field", jsonFields);

    JsonObject oldObj = fieldsState.getAsJsonObject();
    for (Map.Entry<String, JsonElement> entry : oldObj.entrySet()) {
      jsonFields.add(entry.getValue());
    }
    return newObj.toString();
  }

  public SettingsRequest buildSettingsRequest(String jsonStr) {
    logger.info(String.format("Converting fields %s to proto SettingsRequest", jsonStr));
    SettingsRequest.Builder settingsRequestBuilder = SettingsRequest.newBuilder();
    if (!jsonStr.isEmpty()) {
      try {
        JsonFormat.parser().merge(jsonStr, settingsRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
    // set defaults
    if (settingsRequestBuilder.getNrtCachingDirectoryMaxMergeSizeMB() == 0) {
      settingsRequestBuilder.setNrtCachingDirectoryMaxMergeSizeMB(5.0);
    }
    if (settingsRequestBuilder.getNrtCachingDirectoryMaxSizeMB() == 0) {
      settingsRequestBuilder.setNrtCachingDirectoryMaxSizeMB(60.0);
    }
    if (settingsRequestBuilder.getDirectory().isEmpty()) {
      settingsRequestBuilder.setDirectory("FSDirectory");
    }
    if (settingsRequestBuilder.getNormsFormat().isEmpty()) {
      settingsRequestBuilder.setNormsFormat("Lucene80");
    }
    // set indexName which is not present in the jsonStr from state
    settingsRequestBuilder.setIndexName(getName());
    SettingsRequest settingsRequest = settingsRequestBuilder.build();
    logger.info(String.format("jsonStr converted to proto SettingsRequest: \n%s", settingsRequest));
    return settingsRequest;
  }

  public LiveSettingsRequest buildLiveSettingsRequest(String jsonStr) {
    logger.info(String.format("Converting fields %s to proto LiveSettingsRequest", jsonStr));
    LiveSettingsRequest.Builder liveSettingsRequestBuilder = LiveSettingsRequest.newBuilder();
    if (!jsonStr.isEmpty()) {
      try {
        JsonFormat.parser().merge(jsonStr, liveSettingsRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
    // set defaults
    if (liveSettingsRequestBuilder.getMaxRefreshSec() == 0) {
      liveSettingsRequestBuilder.setMaxRefreshSec(1.0);
    }
    if (liveSettingsRequestBuilder.getMinRefreshSec() == 0) {
      liveSettingsRequestBuilder.setMinRefreshSec(0.5);
    }
    if (liveSettingsRequestBuilder.getMaxSearcherAgeSec() == 0) {
      liveSettingsRequestBuilder.setMaxSearcherAgeSec(60.0);
    }
    if (liveSettingsRequestBuilder.getIndexRamBufferSizeMB() == 0) {
      liveSettingsRequestBuilder.setIndexRamBufferSizeMB(250);
    }
    if (liveSettingsRequestBuilder.getAddDocumentsMaxBufferLen() == 0) {
      liveSettingsRequestBuilder.setAddDocumentsMaxBufferLen(100);
    }

    // set indexName which is not present in the jsonStr from state
    liveSettingsRequestBuilder.setIndexName(getName());
    LiveSettingsRequest liveSettingsRequest = liveSettingsRequestBuilder.build();
    logger.info(
        String.format("jsonStr converted to proto LiveSettingsRequest: \n%s", liveSettingsRequest));
    return liveSettingsRequest;
  }

  /** Returns all field names that are indexed and analyzed. */
  @Override
  public List<String> getIndexedAnalyzedFields() {
    List<String> result = new ArrayList<>();
    for (FieldDef fd : fields.values()) {
      // TODO: should we default to include numeric fields too...?
      if (fd instanceof TextBaseFieldDef) {
        if (((TextBaseFieldDef) fd).getSearchAnalyzer().isPresent()) {
          result.add(fd.getName());
        }
      }
    }

    return result;
  }

  @Override
  public Map<String, FieldDef> getEagerGlobalOrdinalFields() {
    return eagerGlobalOrdinalFields;
  }

  @Override
  public Bindings getExpressionBindings() {
    return exprBindings;
  }

  /** Records a new suggester state. */
  @Override
  public void addSuggest(String name, JsonObject o) {
    saveState.getSuggest().add(name, o);
  }

  @Override
  public Map<String, Lookup> getSuggesters() {
    return suggesters;
  }
}
