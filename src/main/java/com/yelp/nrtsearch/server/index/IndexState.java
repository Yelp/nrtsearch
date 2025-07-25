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
package com.yelp.nrtsearch.server.index;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.SegmentDocLookup;
import com.yelp.nrtsearch.server.field.ContextSuggestFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.field.TextBaseFieldDef;
import com.yelp.nrtsearch.server.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.FileUtils;
import com.yelp.nrtsearch.server.warming.Warmer;
import com.yelp.nrtsearch.server.warming.WarmerConfig;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
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
public abstract class IndexState implements Closeable {
  public static final String CHILD_FIELD_SEPARATOR = ".";

  public static final String NESTED_PATH = "_nested_path";
  public static final String ROOT = "_root";
  public static final String FIELD_NAMES = "_field_names";
  public static final String NESTED_DOCUMENT_OFFSET = "_parent_offset";

  private static final Logger logger = LoggerFactory.getLogger(IndexState.class);
  private final GlobalState globalState;

  private final String name;
  private final Path rootDir;

  private static final Pattern reSimpleName = Pattern.compile("^[a-zA-Z_][a-zA-Z_0-9]*$");
  private final ExecutorService searchExecutor;
  private Warmer warmer = null;

  /** The meta field definitions */
  private static Map<String, FieldDef> metaFields;

  /**
   * Index level doc values lookup. Generates {@link SegmentDocLookup} for a given lucene segment.
   */
  public final DocLookup docLookup = new DocLookup(this::getField);

  /**
   * Holds the configuration for parallel fetch operations.
   *
   * @param maxParallelism maximum number of parallel fetch operations
   * @param parallelFetchByField if true, fetches are parallelized by field instead of by document
   * @param parallelFetchChunkSize number of documents/fields in each parallel fetch operation
   * @param fetchExecutor executor service for parallel fetch operations
   */
  public record ParallelFetchConfig(
      int maxParallelism,
      boolean parallelFetchByField,
      int parallelFetchChunkSize,
      ExecutorService fetchExecutor) {}

  /** Search-time analyzer. */
  public final Analyzer searchAnalyzer =
      new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        public Analyzer getWrappedAnalyzer(String name) {
          FieldDef fd = getFieldOrThrow(name);
          if (fd instanceof TextBaseFieldDef) {
            Optional<Analyzer> maybeAnalyzer = ((TextBaseFieldDef) fd).getSearchAnalyzer();
            if (maybeAnalyzer.isEmpty()) {
              throw new IllegalArgumentException(
                  "field \"" + name + "\" did not specify analyzer or searchAnalyzer");
            }
            return maybeAnalyzer.get();
          } else if (fd instanceof ContextSuggestFieldDef) {
            Optional<Analyzer> maybeAnalyzer = ((ContextSuggestFieldDef) fd).getSearchAnalyzer();
            if (maybeAnalyzer.isEmpty()) {
              throw new IllegalArgumentException(
                  "field \"" + name + "\" did not specify analyzer or searchAnalyzer");
            }
            return maybeAnalyzer.get();
          }
          throw new IllegalArgumentException("field \"" + name + "\" does not support analysis");
        }
      };

  /** Per-field wrapper that provides the similarity for searcher */
  public final Similarity searchSimilarity =
      new PerFieldSimilarityWrapper() {
        @Override
        public Similarity get(String name) {
          return IndexSimilarity.getFromState(name, IndexState.this);
        }
      };

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

    /** Initialize with an id * */
    public Gens(String id) {
      this(id, "id");
    }

    /** Initialize with a custom param (which is unused) */
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

  /**
   * Retrieve the meta field's type.
   *
   * @throws IllegalArgumentException if the field was not valid.
   */
  public static FieldDef getMetaField(String fieldName) {
    FieldDef fd = metaFields.get(fieldName);
    if (fd == null) {
      String message = "field \"" + fieldName + "\" is unknown: it was not a valid meta field";
      throw new IllegalArgumentException(message);
    }
    return fd;
  }

  public static Map<String, FieldDef> getMetaFields() {
    return metaFields;
  }

  /** Verifies this name doesn't use any "exotic" characters. */
  public static boolean isSimpleName(String name) {
    return reSimpleName.matcher(name).matches();
  }

  /**
   * Sole constructor; creates a new index or loads an existing one if it exists, but does not start
   * the index.
   */
  public IndexState(GlobalState globalState, String name, Path rootDir) throws IOException {
    this.globalState = globalState;
    this.name = name;
    this.rootDir = rootDir;

    // add meta data fields
    metaFields = getPredefinedMetaFields(globalState);

    if (!Files.exists(rootDir)) {
      Files.createDirectories(rootDir);
    }

    searchExecutor = globalState.getSearchExecutor();
  }

  /** Get index name. */
  public String getName() {
    return name;
  }

  /** Get global state */
  public GlobalState getGlobalState() {
    return globalState;
  }

  /** Get the root directory for all index data. */
  public Path getRootDir() {
    return rootDir;
  }

  public void initWarmer(RemoteBackend remoteBackend) {
    initWarmer(remoteBackend, name);
  }

  public void initWarmer(RemoteBackend remoteBackend, String indexName) {
    NrtsearchConfig configuration = globalState.getConfiguration();
    WarmerConfig warmerConfig = configuration.getWarmerConfig();
    if (warmerConfig.isWarmOnStartup() || warmerConfig.getMaxWarmingQueries() > 0) {
      this.warmer =
          new Warmer(
              remoteBackend,
              configuration.getServiceName(),
              indexName,
              warmerConfig.getMaxWarmingQueries(),
              warmerConfig.getWarmBasicQueryOnlyPerc());
    }
  }

  /**
   * Deletes the Index's root directory
   *
   * @throws IOException
   */
  public void deleteIndexRootDir() throws IOException {
    if (rootDir != null) {
      FileUtils.deleteAllFiles(rootDir);
    }
  }

  /** Get executor to use for search operations. */
  public ExecutorService getSearchExecutor() {
    return searchExecutor;
  }

  public ThreadPoolConfiguration getThreadPoolConfiguration() {
    return globalState.getThreadPoolConfiguration();
  }

  /** Get query warmer to use during index start. */
  public Warmer getWarmer() {
    return warmer;
  }

  /**
   * Verify the index is started, throwing an exception if it is not.
   *
   * @throws IllegalStateException if index is not started
   */
  public void verifyStarted() {
    if (!isStarted()) {
      String message = "index '" + name + "' isn't started; call startIndex first";
      throw new IllegalStateException(message);
    }
  }

  /**
   * resolve the nested object path, and do validation if it is not _root.
   *
   * @param path path of the nested object
   * @return resolved path
   * @throws IllegalArgumentException if the non-root path is invalid
   */
  public String resolveQueryNestedPath(String path) {
    if (path == null || path.isEmpty() || path.equals(IndexState.ROOT)) {
      return IndexState.ROOT;
    }
    FieldDef fieldDef = getFieldOrThrow(path);
    if ((fieldDef instanceof ObjectFieldDef) && ((ObjectFieldDef) fieldDef).isNestedDoc()) {
      return path;
    }
    throw new IllegalArgumentException("Nested path is not a nested object field: " + path);
  }

  /**
   * Get the base path for the nested document containing the field at the given path. For fields in
   * the base document, this returns _root. The base nested path for _root is null.
   *
   * @param path field path
   * @return nested base path, or null
   */
  public static String getFieldBaseNestedPath(String path, IndexState indexState) {
    Objects.requireNonNull(path, "path cannot be null");
    if (path.equals(IndexState.ROOT)) {
      return null;
    }

    String currentPath = path;
    while (currentPath.contains(".")) {
      currentPath = currentPath.substring(0, currentPath.lastIndexOf("."));
      FieldDef fieldDef = indexState.getFieldOrThrow(currentPath);
      if (fieldDef instanceof ObjectFieldDef objFieldDef && objFieldDef.isNestedDoc()) {
        return currentPath;
      }
    }
    return IndexState.ROOT;
  }

  /** Get index state info. */
  public abstract IndexStateInfo getIndexStateInfo();

  /** Get if the index is started. */
  public abstract boolean isStarted();

  /**
   * Start index in the given mode.
   *
   * @param serverMode server mode
   * @param nrtDataManager manager for loading and saving of remote nrt point data
   * @param primaryGen primary generation, only valid for PRIMARY or REPLICA modes
   * @param primaryClient replication client for talking with primary, only valid for REPLICA mode
   * @throws IOException on filesystem error
   */
  public abstract void start(
      Mode serverMode,
      NrtDataManager nrtDataManager,
      long primaryGen,
      ReplicationServerClient primaryClient)
      throws IOException;

  /**
   * Retrieve definition of field by name.
   *
   * @param fieldName name of the field
   * @return field definition or null if the field does not exist
   */
  public abstract FieldDef getField(String fieldName);

  /**
   * Retrieve definition of field by name. Throws an exception if the field does not exist.
   *
   * @param fieldName name of the field
   * @return field definition
   * @throws IllegalArgumentException if the field does not exist.
   */
  public FieldDef getFieldOrThrow(String fieldName) {
    FieldDef fieldDef = getField(fieldName);
    if (fieldDef == null) {
      throw new IllegalArgumentException("field \"" + fieldName + "\" is unknown");
    }
    return fieldDef;
  }

  /** Get all registered fields. */
  public abstract Map<String, FieldDef> getAllFields();

  /** Get json string representation of all registered fields. */
  public abstract String getAllFieldsJSON();

  /** Get id field definition, if one is registered for this index. */
  public abstract Optional<IdFieldDef> getIdFieldDef();

  /** Returns all field names that are indexed and analyzed. */
  public abstract List<String> getIndexedAnalyzedFields();

  /** Get fields with facets that do eager global ordinal building. */
  public abstract Map<String, FieldDef> getEagerGlobalOrdinalFields();

  /** Get fields with doc values that do eager global ordinal building. */
  public abstract Map<String, GlobalOrdinalable> getEagerFieldGlobalOrdinalFields();

  /** Verifies if it has nested child object fields. */
  public abstract boolean hasNestedChildFields();

  public abstract boolean hasFacets();

  public abstract Set<String> getInternalFacetFieldNames();

  public abstract FacetsConfig getFacetsConfig();

  /**
   * Get configuration for parallel fetch for this index.
   *
   * @return configuration for parallel fetch
   */
  public abstract ParallelFetchConfig getParallelFetchConfig();

  /**
   * Get shard state.
   *
   * @param shardOrd shard ordinal
   * @return shard state
   */
  public abstract ShardState getShard(int shardOrd);

  /** Get mapping of ordinal to shard state. */
  public abstract Map<Integer, ShardState> getShards();

  /** Commit all state and shards */
  public abstract long commit() throws IOException;

  /** True if this index has at least one commit. */
  public abstract boolean hasCommit() throws IOException;

  /** Delete all index data and state. */
  public abstract void deleteIndex() throws IOException;

  /** Get config to use when opening index for writing. */
  public abstract IndexWriterConfig getIndexWriterConfig(
      IndexWriterConfig.OpenMode openMode, Directory origIndexDir, int shardOrd) throws IOException;

  /** Record that this snapshot id refers to the current generation, returning it. */
  public abstract long incRefLastCommitGen() throws IOException;

  /** Drop this snapshot from the references. */
  public abstract void decRef(long stateGen) throws IOException;

  /** Get number of snapshots referencing each index generation. */
  public abstract Map<Long, Integer> getGenRefCounts();

  /** Get the current save state. */
  public abstract JsonObject getSaveState() throws IOException;

  // Settings

  /** Get lucene Directory factory to use for index data. */
  public abstract DirectoryFactory getDirectoryFactory();

  /** Max size of merged segment that should use the nrt caching directory wrapper. */
  public abstract double getNrtCachingDirectoryMaxMergeSizeMB();

  /** Max size to use for nrt caching directory wrapper. */
  public abstract double getNrtCachingDirectoryMaxSizeMB();

  // Live Settings

  /** Min time before the IndexSearch is automatically re-opened. */
  public abstract double getMinRefreshSec();

  /** Max time before the IndexSearch is automatically re-opened. */
  public abstract double getMaxRefreshSec();

  /** Max time to wait before pruning stale searchers. */
  public abstract double getMaxSearcherAgeSec();

  /**
   * Max document data the {@link org.apache.lucene.index.IndexWriter} should buffer before
   * flushing.
   */
  public abstract double getIndexRamBufferSizeMB();

  /** Live setting: max number of documents to add at a time. */
  public abstract int getAddDocumentsMaxBufferLen();

  /** Get the maximum docs per parallel search slice. */
  public abstract int getSliceMaxDocs();

  /** Get the maximum segments per parallel search slice. */
  public abstract int getSliceMaxSegments();

  /**
   * Get the number of virtual shards for this index. If virtual sharding is disabled, this always
   * returns 1.
   */
  public abstract int getVirtualShards();

  /** Get maximum sized segment to produce during normal merging */
  public abstract int getMaxMergedSegmentMB();

  /** Get the number of segments per tier used by merge policy, or 0 if using policy default. */
  public abstract int getSegmentsPerTier();

  /** Get the maximum percentage of deleted documents that is tolerated in the index. */
  public abstract double getDeletePctAllowed();

  /** Get the default search timeout. */
  public abstract double getDefaultSearchTimeoutSec();

  /** Get the default terminate after. */
  public abstract int getDefaultTerminateAfter();

  /** Get the default terminate after max recall count. */
  public abstract int getDefaultTerminateAfterMaxRecallCount();

  /** Get the default search timeout check every. */
  public abstract int getDefaultSearchTimeoutCheckEvery();

  /** Get the max merge precopy duration (in seconds). */
  public abstract long getMaxMergePreCopyDurationSec();

  /** Get if additional index metrics should be collected and published. */
  public abstract boolean getVerboseMetrics();

  /** Maximum time in milliseconds to wait for merges when doing a full flush. */
  public abstract long getMaxFullFlushMergeWaitMillis();

  @Override
  public void close() throws IOException {}

  // Get all predifined meta fields
  private static Map<String, FieldDef> getPredefinedMetaFields(GlobalState globalState) {
    return ImmutableMap.of(
        NESTED_PATH,
        FieldDefCreator.getInstance()
            .createFieldDef(
                NESTED_PATH,
                Field.newBuilder()
                    .setName(IndexState.NESTED_PATH)
                    .setType(FieldType.ATOM)
                    .setSearch(true)
                    .build(),
                FieldDefCreator.createContext(globalState)),
        FIELD_NAMES,
        FieldDefCreator.getInstance()
            .createFieldDef(
                FIELD_NAMES,
                Field.newBuilder()
                    .setName(FIELD_NAMES)
                    .setType(FieldType.ATOM)
                    .setSearch(true)
                    .setMultiValued(true)
                    .build(),
                FieldDefCreator.createContext(globalState)),
        NESTED_DOCUMENT_OFFSET,
        FieldDefCreator.getInstance()
            .createFieldDef(
                NESTED_DOCUMENT_OFFSET,
                Field.newBuilder()
                    .setName(NESTED_DOCUMENT_OFFSET)
                    .setType(FieldType.INT)
                    .setStoreDocValues(true)
                    .build(),
                FieldDefCreator.createContext(globalState)));
  }
}
