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
package com.yelp.nrtsearch.server.luceneserver;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.SettingsRequest;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefBindings;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.PersistentSnapshotDeletionPolicy;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
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
public class IndexState implements Closeable, Restorable {

  Logger logger = LoggerFactory.getLogger(IndexState.class);
  public final GlobalState globalState;

  /** Which norms format to use for all indexed fields. */
  public String normsFormat = "Lucene80";

  /** If normsFormat is Lucene80, what acceptableOverheadRatio to pass. */
  public float normsAcceptableOverheadRatio = PackedInts.FASTEST;

  public final String name;
  /** Creates directories */
  public DirectoryFactory df = DirectoryFactory.get("MMapDirectory");

  public final Path rootDir;

  /** Optional index time sorting (write once!) or null if no index time sorting */
  private Sort indexSort;

  private final FilesystemResourceLoader resourceLoader;
  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  /** Holds pending save state, written to state.N file on commit. */
  // nocommit move these to their own obj, make it sync'd,
  // instead of syncing on IndexState instance:
  final JsonObject settingsSaveState = new JsonObject();

  final JsonObject liveSettingsSaveState = new JsonObject();
  final JsonObject fieldsSaveState = new JsonObject();
  final JsonObject suggestSaveState = new JsonObject();

  private static final Pattern reSimpleName = Pattern.compile("^[a-zA-Z_][a-zA-Z_0-9]*$");
  private ThreadPoolExecutor searchThreadPoolExecutor;
  private IdFieldDef idFieldDef = null;

  public ShardState addShard(int shardOrd, boolean doCreate) {
    if (shards.containsKey(shardOrd)) {
      throw new IllegalArgumentException(
          "shardOrd=" + shardOrd + " already exists in index + \"" + name + "\"");
    }
    ShardState shard = new ShardState(this, shardOrd, doCreate);
    // nocommit fail if there is already a shard here?
    shards.put(shardOrd, shard);
    return shard;
  }

  /**
   * Returns the shard that should be used for writing new documents, possibly creating and starting
   * a new shard if the current one is full.
   */
  public ShardState getWritableShard() {
    verifyStarted();
    // TODO: remove this code, why do we need 50000000 per doc limit?
    //        synchronized (this) {
    //
    //            // nocommit make this tunable:
    //            if (writableShard.writer.maxDoc() > 50000000) {
    //                logger.info("NEW SHARD ORD " + (writableShard.shardOrd+1));
    //                ShardState nextShard = addShard(writableShard.shardOrd+1, true);
    //
    //                // nocommit hmm we can't do this now ... we need to get all in-flight ops to
    // finish first
    //                writableShard.finishWriting();
    //                // nocommit we should close IW after forceMerge finishes
    //
    //                nextShard.start();
    //                writableShard = nextShard;
    //            }
    //
    //            writableShard.startIndexingChunk();
    //        }

    return writableShard;
  }

  public void deleteIndex() throws IOException {
    for (ShardState shardState : shards.values()) {
      shardState.deleteShard();
    }
    globalState.deleteIndex(name);
  }

  /** True if this index has at least one commit. */
  public boolean hasCommit() throws IOException {
    return saveLoadState.getNextWriteGen() != 0;
  }

  /** Record that this snapshot id refers to the current generation, returning it. */
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
      genRefCounts.put(stateGen, 1 + rc.intValue());
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  /** Drop this snapshot from the references. */
  public synchronized void decRef(long stateGen) throws IOException {
    Integer rc = genRefCounts.get(stateGen);
    if (rc == null) {
      throw new IllegalArgumentException("stateGen=" + stateGen + " is not held by a snapshot");
    }
    assert rc.intValue() > 0;
    if (rc.intValue() == 1) {
      genRefCounts.remove(stateGen);
    } else {
      genRefCounts.put(stateGen, rc.intValue() - 1);
    }
    saveLoadGenRefCounts.save(genRefCounts);
  }

  public ThreadPoolExecutor getSearchThreadPoolExecutor() {
    return searchThreadPoolExecutor;
  }

  public IdFieldDef getIdFieldDef() {
    return idFieldDef;
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
      byte[] bytes = IndexState.toUTF8(o.toString());
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected Map<Long, Integer> loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, bytes.length);
      String s = IndexState.fromUTF8(bytes);
      JsonObject o = null;
      try {
        o = jsonParser.parse(s).getAsJsonObject();
      } catch (JsonParseException pe) {
        // Change to IOExc so gen logic will fallback:
        throw new IOException("invalid JSON when parsing refCounts", pe);
      }
      Map<Long, Integer> refCounts = new HashMap<Long, Integer>();
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
      byte[] bytes = IndexState.toUTF8(pretty.toString());
      out.writeBytes(bytes, 0, bytes.length);
    }

    @Override
    protected JsonObject loadOne(IndexInput in) throws IOException {
      int numBytes = (int) in.length();
      byte[] bytes = new byte[numBytes];
      in.readBytes(bytes, 0, numBytes);
      String s = IndexState.fromUTF8(bytes);
      JsonObject ret = null;
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

  /** Per-field wrapper that provides the analyzer configured in the FieldDef */
  private static final Analyzer keywordAnalyzer = new KeywordAnalyzer();

  /** Index-time analyzer. */
  public final Analyzer indexAnalyzer =
      new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          FieldDef fd = getField(name);
          if (fd instanceof TextBaseFieldDef) {
            Optional<Analyzer> maybeAnalyzer = ((TextBaseFieldDef) fd).getIndexAnalyzer();
            if (maybeAnalyzer.isEmpty()) {
              throw new IllegalArgumentException(
                  "field \"" + name + "\" did not specify analyzer or indexAnalyzer");
            }
            return maybeAnalyzer.get();
          }
          throw new IllegalArgumentException("field \"" + name + "\" does not support analysis");
        }

        @Override
        protected TokenStreamComponents wrapComponents(
            String fieldName, TokenStreamComponents components) {
          return components;
        }
      };

  /** Search-time analyzer. */
  public final Analyzer searchAnalyzer =
      new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          FieldDef fd = getField(name);
          if (fd instanceof TextBaseFieldDef) {
            Optional<Analyzer> maybeAnalyzer = ((TextBaseFieldDef) fd).getSearchAnalyzer();
            if (maybeAnalyzer.isEmpty()) {
              throw new IllegalArgumentException(
                  "field \"" + name + "\" did not specify analyzer or searchAnalyzer");
            }
            return maybeAnalyzer.get();
          }
          throw new IllegalArgumentException("field \"" + name + "\" does not support analysis");
        }

        @Override
        protected TokenStreamComponents wrapComponents(
            String fieldName, TokenStreamComponents components) {
          return components;
        }
      };

  /** Per-field wrapper that provides the similarity configured in the FieldDef */
  final Similarity sim =
      new PerFieldSimilarityWrapper() {
        final Similarity defaultSim = new BM25Similarity();

        @Override
        public Similarity get(String name) {
          if (fields.containsKey(name)) {
            FieldDef fd = getField(name);
            if (fd instanceof IndexableFieldDef) {
              return ((IndexableFieldDef) fd).getSimilarity();
            }
          }
          return defaultSim;
        }
      };

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

  /** True if this is a new index. */
  private final boolean doCreate;

  /**
   * Sole constructor; creates a new index or loads an existing one if it exists, but does not start
   * the index.
   */
  public IndexState(
      GlobalState globalState, String name, Path rootDir, boolean doCreate, boolean hasRestore)
      throws IOException {
    this.globalState = globalState;
    this.name = name;
    this.rootDir = rootDir;
    // nocommit require rootDir != null!  no RAMDirectory!
    if (rootDir != null) {
      if (Files.exists(rootDir) == false) {
        Files.createDirectories(rootDir);
      }
      this.resourceLoader =
          new FilesystemResourceLoader(rootDir, IndexState.class.getClassLoader());
    } else {
      // nocommit can/should we make a DirectoryResourceLoader?
      this.resourceLoader = null;
    }

    this.doCreate = doCreate;

    if (doCreate == false && !hasRestore) {
      initSaveLoadState();
    }
    searchThreadPoolExecutor = globalState.getSearchThreadPoolExecutor();
  }

  void initSaveLoadState() throws IOException {
    Path stateDirFile;
    if (rootDir != null) {
      stateDirFile = rootDir.resolve("state");
      // if (!stateDirFile.exists()) {
      // stateDirFile.mkdirs();
      // }
    } else {
      stateDirFile = null;
    }

    // nocommit who closes this?
    // nocommit can't this be in the rootDir directly?
    Directory stateDir = df.open(stateDirFile);

    saveLoadGenRefCounts = new SaveLoadRefCounts(stateDir);

    // Snapshot ref counts:
    genRefCounts = saveLoadGenRefCounts.load();
    if (genRefCounts == null) {
      genRefCounts = new HashMap<Long, Integer>();
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
  private final Map<String, FieldDef> fields = new ConcurrentHashMap<String, FieldDef>();

  /** Contains fields set as facetIndexFieldName. */
  public final Set<String> internalFacetFieldNames =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public final FacetsConfig facetsConfig = new FacetsConfig();

  /** {@link Bindings} to pass when evaluating expressions. */
  public final Bindings exprBindings = new FieldDefBindings(fields);

  public final Map<Integer, ShardState> shards = new ConcurrentHashMap<>();

  private ShardState writableShard;

  /** Built suggest implementations */
  public final Map<String, Lookup> suggesters = new ConcurrentHashMap<>();

  /** Holds suggest settings loaded but not yet started */
  private JsonObject suggesterSettings;

  @Override
  public void close() throws IOException {
    logger.info(String.format("IndexState.close name= %s", name));
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

    globalState.indices.remove(name);
  }

  /** True if this generation is still referenced by at least one snapshot. */
  public synchronized boolean hasRef(long gen) {
    Integer rc = genRefCounts.get(gen);
    if (rc == null) {
      return false;
    } else {
      assert rc.intValue() > 0;
      return true;
    }
  }

  /** String -&gt; UTF8 byte[]. */
  public static byte[] toUTF8(String s) {
    CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
    // Make sure we catch any invalid UTF16:
    encoder.onMalformedInput(CodingErrorAction.REPORT);
    encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      ByteBuffer bb = encoder.encode(CharBuffer.wrap(s));
      byte[] bytes = new byte[bb.limit()];
      bb.position(0);
      bb.get(bytes, 0, bytes.length);
      return bytes;
    } catch (CharacterCodingException cce) {
      throw new IllegalArgumentException(cce);
    }
  }

  /** UTF8 byte[] -&gt; String. */
  public static String fromUTF8(byte[] bytes) {
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    // Make sure we catch any invalid UTF8:
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      return decoder.decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException cce) {
      throw new IllegalArgumentException(cce);
    }
  }

  /** Commit all state and shards. */
  public synchronized long commit() throws IOException {

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

    return gen;
  }

  /** Get the current save state. */
  public synchronized JsonObject getSaveState() throws IOException {
    JsonObject o = new JsonObject();
    o.add("settings", settingsSaveState);
    o.add("liveSettings", liveSettingsSaveState);
    o.add("fields", fieldsSaveState);
    o.add("suggest", suggestSaveState);
    return o;
  }

  /**
   * Retrieve the field's type.
   *
   * @throws IllegalArgumentException if the field was not registered.
   */
  public FieldDef getField(String fieldName) {
    FieldDef fd = fields.get(fieldName);
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

  /** Verifies this name doesn't use any "exotic" characters. */
  public static boolean isSimpleName(String name) {
    return reSimpleName.matcher(name).matches();
  }

  /**
   * Live setting: set the mininum refresh time (seconds), which is the longest amount of time a
   * client may wait for a searcher to reopen.
   */
  public synchronized void setMinRefreshSec(double min) {
    minRefreshSec = min;
    liveSettingsSaveState.addProperty("minRefreshSec", min);
    for (ShardState shardState : shards.values()) {
      shardState.restartReopenThread();
    }
  }

  /**
   * Live setting: set the maximum refresh time (seconds), which is the amount of time before we
   * reopen the searcher proactively (when no search client is waiting for a specific index
   * generation).
   */
  public synchronized void setMaxRefreshSec(double max) {
    maxRefreshSec = max;
    liveSettingsSaveState.addProperty("maxRefreshSec", max);
    for (ShardState shardState : shards.values()) {
      shardState.restartReopenThread();
    }
  }

  /** Live setting: once a searcher becomes stale, we will close it after this many seconds. */
  public synchronized void setMaxSearcherAgeSec(double d) {
    maxSearcherAgeSec = d;
    liveSettingsSaveState.addProperty("maxSearcherAgeSec", d);
  }

  /**
   * Live setting: how much RAM to use for buffered documents during indexing (passed to {@link
   * IndexWriterConfig#setRAMBufferSizeMB}.
   */
  public synchronized void setIndexRamBufferSizeMB(double d) {
    indexRamBufferSizeMB = d;
    liveSettingsSaveState.addProperty("indexRamBufferSizeMB", d);

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
    liveSettingsSaveState.addProperty("addDocumentsMaxBufferLen", i);
  }

  /** Live setting: max number of documents to add at a time. */
  public int getAddDocumentsMaxBufferLen() {
    return addDocumentsMaxBufferLen;
  }

  /** Returns JSON representation of all live settings. */
  public synchronized String getLiveSettingsJSON() {
    return liveSettingsSaveState.toString();
  }

  public boolean hasFacets() {
    return !internalFacetFieldNames.isEmpty();
  }

  /** Returns JSON representation of all registered fields. */
  public synchronized String getAllFieldsJSON() {
    return fieldsSaveState.toString();
  }

  /** Returns JSON representation of all settings. */
  public synchronized String getSettingsJSON() {
    return settingsSaveState.toString();
  }

  public Map<String, FieldDef> getAllFields() {
    return Collections.unmodifiableMap(fields);
  }

  /** Records a new field in the internal {@code fields} state. */
  public synchronized void addField(FieldDef fd, JsonObject jsonObject) {
    if (fields.containsKey(fd.getName())) {
      throw new IllegalArgumentException("field \"" + fd.getName() + "\" was already registered");
    }
    fields.put(fd.getName(), fd);
    assert null == fieldsSaveState.get(fd.getName());
    fieldsSaveState.add(fd.getName(), jsonObject);
    // nocommit support sorted set dv facets
    if (fd instanceof IndexableFieldDef) {
      IndexableFieldDef.FacetValueType facetValueType =
          ((IndexableFieldDef) fd).getFacetValueType();
      if (facetValueType != IndexableFieldDef.FacetValueType.NO_FACETS
          && facetValueType != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
        internalFacetFieldNames.add(facetsConfig.getDimConfig(fd.getName()).indexFieldName);
      }
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
          "index \"" + name + "\": cannot change Directory when the index is running");
    }
    settingsSaveState.addProperty("directory", directoryClassName);
    this.df = df;
  }

  public void setIndexSort(Sort sort, JsonObject saveState) {
    if (isStarted()) {
      throw new IllegalStateException(
          "index \"" + name + "\": cannot change index sort when the index is running");
    }
    if (this.indexSort != null && this.indexSort.equals(sort) == false) {
      throw new IllegalStateException("index \"" + name + "\": cannot change index sort");
    }
    settingsSaveState.add("indexSort", saveState);
    this.indexSort = sort;
  }

  public void verifyStarted() {
    if (isStarted() == false) {
      String message = "index '" + name + "' isn't started; call startIndex first";
      throw new IllegalStateException(message);
    }
  }

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
          "index \"" + name + "\" was already started (cannot change non-live settings)");
    }
    settingsSaveState.addProperty("mergeMaxMBPerSec", settingsRequest.getMergeMaxMBPerSec());
    settingsSaveState.addProperty(
        "nrtCachingDirectoryMaxMergeSizeMB",
        settingsRequest.getNrtCachingDirectoryMaxMergeSizeMB());
    settingsSaveState.addProperty(
        "nrtCachingDirectoryMaxSizeMB", settingsRequest.getNrtCachingDirectoryMaxSizeMB());
    if ((settingsRequest.getConcurrentMergeSchedulerMaxThreadCount() != 0)
        && settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
      settingsSaveState.addProperty(
          "concurrentMergeSchedulerMaxThreadCount",
          settingsRequest.getConcurrentMergeSchedulerMaxThreadCount());
      settingsSaveState.addProperty(
          "concurrentMergeSchedulerMaxMergeCount",
          settingsRequest.getConcurrentMergeSchedulerMaxMergeCount());
    }
    settingsSaveState.addProperty("indexVerbose", settingsRequest.getIndexVerbose());
    settingsSaveState.addProperty(
        "indexMergeSchedulerAutoThrottle", settingsRequest.getIndexMergeSchedulerAutoThrottle());
  }

  public synchronized void start(Path dataPath) throws Exception {
    if (!doCreate && dataPath != null) {
      if (rootDir != null) {
        synchronized (this) {
          // copy downloaded data into rootDir
          restoreDir(dataPath, rootDir);
        }
        initSaveLoadState();
      }
    }

    // start all local shards
    for (ShardState shard : shards.values()) {
      shard.start();
      if (writableShard == null || shard.shardOrd > writableShard.shardOrd) {
        writableShard = shard;
      }
    }

    if (suggesterSettings != null) {
      // load suggesters:
      new BuildSuggestHandler(searchThreadPoolExecutor).load(this, suggesterSettings);
      suggesterSettings = null;
    }
  }

  IndexWriterConfig getIndexWriterConfig(
      IndexWriterConfig.OpenMode openMode, Directory origIndexDir, int shardOrd)
      throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer);
    iwc.setOpenMode(openMode);
    if (getBooleanSetting("indexVerbose", false)) {
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
            thread.setName(thread.getName() + " [" + name + ":" + shardOrd + "]");
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

    iwc.setCodec(new ServerCodec(this));
    return iwc;
  }

  synchronized boolean getBooleanSetting(String name, boolean val) {
    return settingsSaveState.get(name) == null ? val : settingsSaveState.get(name).getAsBoolean();
  }

  synchronized double getDoubleSetting(String name, double val) {
    return settingsSaveState.get(name) == null ? val : settingsSaveState.get(name).getAsDouble();
  }

  synchronized int getIntSetting(String name) {
    return settingsSaveState.get(name).getAsInt();
  }

  synchronized boolean hasSetting(String name) {
    return settingsSaveState.get(name) != null;
  }

  public ShardState getShard(int shardOrd) {
    ShardState shardState = shards.get(shardOrd);
    if (shardState == null) {
      throw new IllegalArgumentException(
          "shardOrd=" + shardOrd + " does not exist in index \"" + name + "\"");
    }
    return shardState;
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
    logger.info(
        String.format("jsonStr converted to proto FieldDefRequest %s", fieldDefRequest.toString()));
    return fieldDefRequest;
  }

  // Builds the valid Json format for FieldDefRequest from fieldsState json
  private String convertFieldStateToJsonFieldDefRequest(JsonElement fieldsState) {
    JsonObject newObj = new JsonObject();
    // set indexName
    newObj.addProperty("indexName", this.name);
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
    settingsRequestBuilder.setIndexName(this.name);
    SettingsRequest settingsRequest = settingsRequestBuilder.build();
    logger.info(
        String.format(
            "jsonStr converted to proto SettingsRequest: \n%s", settingsRequest.toString()));
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
    liveSettingsRequestBuilder.setIndexName(this.name);
    LiveSettingsRequest liveSettingsRequest = liveSettingsRequestBuilder.build();
    logger.info(
        String.format(
            "jsonStr converted to proto LiveSettingsRequest: \n%s",
            liveSettingsRequest.toString()));
    return liveSettingsRequest;
  }

  /** Returns all field names that are indexed and analyzed. */
  public List<String> getIndexedAnalyzedFields() {
    List<String> result = new ArrayList<String>();
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

  /**
   * Holds metadata for one snapshot, including its id, and the index, taxonomy and state
   * generations.
   */
  public static class Gens {

    /** Index generation. */
    public final long indexGen;

    /** Taxonomy index generation. */
    public final long taxoGen;

    /** State generation. */
    public final long stateGen;

    /** Snapshot id. */
    public final String id;

    /** Sole constructor. */
    public Gens(String id, String param) {
      this.id = id;
      final String[] gens = id.split(":");
      if (gens.length != 3) {
        throw new RuntimeException("invalid snapshot id \"" + id + "\": must be format N:M:O");
      }
      long indexGen1 = -1;
      long taxoGen1 = -1;
      long stateGen1 = -1;
      try {
        indexGen1 = Long.parseLong(gens[0]);
        taxoGen1 = Long.parseLong(gens[1]);
        stateGen1 = Long.parseLong(gens[2]);
      } catch (Exception e) {
        throw new RuntimeException("invalid snapshot id \"" + id + "\": must be format N:M:O");
      }
      indexGen = indexGen1;
      taxoGen = taxoGen1;
      stateGen = stateGen1;
    }
  }

  /** Records a new suggester state. */
  public synchronized void addSuggest(String name, JsonObject o) {
    suggestSaveState.add(name, o);
  }

  /**
   * Index level doc values lookup. Generates {@link
   * com.yelp.nrtsearch.server.luceneserver.doc.SegmentDocLookup} for a given lucene segment.
   */
  public final DocLookup docLookup = new DocLookup(this);
}
