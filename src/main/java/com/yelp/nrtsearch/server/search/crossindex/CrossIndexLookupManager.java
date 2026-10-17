/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.crossindex;

import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.IntFieldDef;
import com.yelp.nrtsearch.server.field.LongFieldDef;
import com.yelp.nrtsearch.server.field.properties.TermQueryable;
import com.yelp.nrtsearch.server.grpc.CrossIndexLookup;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages cross-index lookups: acquires secondary searchers, materializes field data from secondary
 * indices after collection, and populates SharedDocContext for rescoring scripts.
 *
 * <p>Lifecycle: created during request processing, materialize() called between collection and
 * rescoring, close() called in the finally block.
 */
public class CrossIndexLookupManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CrossIndexLookupManager.class);
  private static final int DEFAULT_TOP_HITS = 3;

  private final Map<String, LookupState> lookupStates;
  private final Executor searchExecutor;

  /** Check if a FieldDef is an allowed exact-key join field type. */
  static boolean isAllowedJoinFieldType(FieldDef fd) {
    return fd instanceof AtomFieldDef
        || fd instanceof IdFieldDef
        || fd instanceof IntFieldDef
        || fd instanceof LongFieldDef;
  }

  /** Validated config holder used between validation pass and searcher acquisition pass. */
  private record ValidatedLookup(
      CrossIndexLookup config,
      IndexState secondaryIndex,
      ShardState secondaryShard,
      IndexableFieldDef<?> primaryFieldDef,
      IndexableFieldDef<?> secondaryFieldDef,
      Map<String, FieldDef> fieldsToRead) {}

  /** Internal state for a single cross-index lookup. */
  private static class LookupState {
    final CrossIndexLookup config;
    final IndexState secondaryIndex;
    final ShardState secondaryShard;
    final SearcherAndTaxonomy searcher;
    final IndexableFieldDef<?> primaryFieldDef;
    final IndexableFieldDef<?> secondaryFieldDef;
    final Map<String, FieldDef> fieldsToRead; // union of retrieve_fields + expose_to_scripts
    // Materialized results: join key -> list of secondary hit field values
    Map<String, List<Map<String, CompositeFieldValue>>> materializedResults;

    LookupState(
        CrossIndexLookup config,
        IndexState secondaryIndex,
        ShardState secondaryShard,
        SearcherAndTaxonomy searcher,
        IndexableFieldDef<?> primaryFieldDef,
        IndexableFieldDef<?> secondaryFieldDef,
        Map<String, FieldDef> fieldsToRead) {
      this.config = config;
      this.secondaryIndex = secondaryIndex;
      this.secondaryShard = secondaryShard;
      this.searcher = searcher;
      this.primaryFieldDef = primaryFieldDef;
      this.secondaryFieldDef = secondaryFieldDef;
      this.fieldsToRead = fieldsToRead;
    }
  }

  private CrossIndexLookupManager(Map<String, LookupState> lookupStates, Executor searchExecutor) {
    this.lookupStates = lookupStates;
    this.searchExecutor = searchExecutor;
  }

  /**
   * Create a CrossIndexLookupManager from the search request's cross_index_lookups config.
   *
   * @param lookups the cross-index lookup configs from SearchRequest
   * @param primaryIndex the primary index state (for resolving primary_field)
   * @param globalState global state for acquiring secondary index searchers
   * @return the manager, or null if no lookups are configured
   */
  public static CrossIndexLookupManager create(
      List<CrossIndexLookup> lookups, IndexState primaryIndex, GlobalState globalState) {
    if (lookups == null || lookups.isEmpty()) {
      return null;
    }

    // Pass 1: Validate all lookups before acquiring any searchers.
    Set<String> seenIndexNames = new HashSet<>();
    List<ValidatedLookup> validated = new ArrayList<>();

    for (CrossIndexLookup lookup : lookups) {
      String indexName = lookup.getIndex();
      if (indexName.isEmpty()) {
        throw new IllegalArgumentException("CrossIndexLookup.index must not be empty");
      }
      if (!seenIndexNames.add(indexName)) {
        throw new IllegalArgumentException("Duplicate CrossIndexLookup for index: " + indexName);
      }
      if (lookup.getPrimaryField().isEmpty()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup.primary_field must not be empty for index: " + indexName);
      }
      if (lookup.getSecondaryField().isEmpty()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup.secondary_field must not be empty for index: " + indexName);
      }

      // Validate primary field: allowed type, doc values, single-valued
      FieldDef primaryFieldDefRaw =
          primaryIndex.docLookup.getFieldDefOrThrow(lookup.getPrimaryField());
      if (!(primaryFieldDefRaw instanceof IndexableFieldDef<?> primaryFieldDef)) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: primary_field \""
                + lookup.getPrimaryField()
                + "\" must be an IndexableFieldDef");
      }
      if (!isAllowedJoinFieldType(primaryFieldDefRaw)) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: primary_field \""
                + lookup.getPrimaryField()
                + "\" is not a supported join field type (must be ATOM, ID, INT, or LONG)");
      }
      if (!primaryFieldDef.hasDocValues()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: primary_field \""
                + lookup.getPrimaryField()
                + "\" must have doc values enabled");
      }
      if (primaryFieldDef.isMultiValue()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: primary_field \""
                + lookup.getPrimaryField()
                + "\" must be single-valued");
      }

      // Resolve secondary index
      IndexState secondaryIndex;
      try {
        secondaryIndex = globalState.getIndexOrThrow(indexName);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary index \"" + indexName + "\" not found", e);
      }

      // Validate secondary field: allowed type, same type as primary, searchable, doc values,
      // single-valued
      FieldDef secondaryFieldDefRaw =
          secondaryIndex.docLookup.getFieldDefOrThrow(lookup.getSecondaryField());
      if (!(secondaryFieldDefRaw instanceof IndexableFieldDef<?> secondaryFieldDef)) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary_field \""
                + lookup.getSecondaryField()
                + "\" must be an IndexableFieldDef");
      }
      if (!isAllowedJoinFieldType(secondaryFieldDefRaw)) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary_field \""
                + lookup.getSecondaryField()
                + "\" is not a supported join field type (must be ATOM, ID, INT, or LONG)");
      }
      if (!primaryFieldDefRaw.getClass().equals(secondaryFieldDefRaw.getClass())) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: primary_field \""
                + lookup.getPrimaryField()
                + "\" and secondary_field \""
                + lookup.getSecondaryField()
                + "\" must be the same field type");
      }
      if (!secondaryFieldDef.isSearchable()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary_field \""
                + lookup.getSecondaryField()
                + "\" must be searchable");
      }
      if (!secondaryFieldDef.hasDocValues()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary_field \""
                + lookup.getSecondaryField()
                + "\" must have doc values enabled");
      }
      if (secondaryFieldDef.isMultiValue()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary_field \""
                + lookup.getSecondaryField()
                + "\" must be single-valued");
      }

      // Validate retrieve/expose fields: all must be IndexableFieldDef with doc values
      Map<String, FieldDef> fieldsToRead = new LinkedHashMap<>();
      for (String field : lookup.getRetrieveFieldsList()) {
        FieldDef fd = secondaryIndex.docLookup.getFieldDefOrThrow(field);
        if (!(fd instanceof IndexableFieldDef<?> ifd) || !ifd.hasDocValues()) {
          throw new IllegalArgumentException(
              "CrossIndexLookup: retrieve_field \""
                  + field
                  + "\" must be an IndexableFieldDef with doc values");
        }
        fieldsToRead.put(field, fd);
      }
      for (String field : lookup.getExposeToScriptsList()) {
        if (!fieldsToRead.containsKey(field)) {
          FieldDef fd = secondaryIndex.docLookup.getFieldDefOrThrow(field);
          if (!(fd instanceof IndexableFieldDef<?> ifd) || !ifd.hasDocValues()) {
            throw new IllegalArgumentException(
                "CrossIndexLookup: expose_to_scripts field \""
                    + field
                    + "\" must be an IndexableFieldDef with doc values");
          }
          fieldsToRead.put(field, fd);
        }
      }

      ShardState secondaryShard = secondaryIndex.getShard(0);
      validated.add(
          new ValidatedLookup(
              lookup,
              secondaryIndex,
              secondaryShard,
              primaryFieldDef,
              secondaryFieldDef,
              fieldsToRead));
    }

    // Pass 2: Acquire searchers (all validation passed).
    Map<String, LookupState> states = new LinkedHashMap<>();
    for (ValidatedLookup v : validated) {
      SearcherAndTaxonomy searcher;
      try {
        searcher = v.secondaryShard().acquire();
      } catch (Exception e) {
        // Release any already-acquired searchers before throwing
        for (LookupState state : states.values()) {
          try {
            state.secondaryShard.release(state.searcher);
          } catch (Exception releaseEx) {
            logger.error("Failed to release searcher during cleanup", releaseEx);
          }
        }
        throw new IllegalStateException(
            "CrossIndexLookup: failed to acquire searcher for index \""
                + v.config().getIndex()
                + "\"",
            e);
      }

      states.put(
          v.config().getIndex(),
          new LookupState(
              v.config(),
              v.secondaryIndex(),
              v.secondaryShard(),
              searcher,
              v.primaryFieldDef(),
              v.secondaryFieldDef(),
              v.fieldsToRead()));
    }

    return new CrossIndexLookupManager(states, globalState.getSearchExecutor());
  }

  /**
   * Get the pre-acquired searcher for a secondary index, if a lookup exists for it. Used by
   * CrossIndexQuery to share the searcher instead of acquiring a separate one.
   */
  public SearcherAndTaxonomy getSearcher(String indexName) {
    LookupState state = lookupStates.get(indexName);
    return state != null ? state.searcher : null;
  }

  /** Get the ShardState for a secondary index lookup. */
  public ShardState getShardState(String indexName) {
    LookupState state = lookupStates.get(indexName);
    return state != null ? state.secondaryShard : null;
  }

  /** Check if a lookup exists for the given index. */
  public boolean hasLookup(String indexName) {
    return lookupStates.containsKey(indexName);
  }

  /**
   * Get the materialized results for a given index and join key.
   *
   * @return list of secondary hit field maps, or null if not materialized or no match
   */
  public List<Map<String, CompositeFieldValue>> getResults(String indexName, String joinKey) {
    LookupState state = lookupStates.get(indexName);
    if (state == null || state.materializedResults == null) {
      return null;
    }
    return state.materializedResults.get(joinKey);
  }

  /**
   * Materialize secondary data for the given primary hits. For each primary hit, reads the join key
   * from primary doc values, then searches the secondary index to find matching documents and reads
   * their field values inline during collection.
   *
   * <p>After materialization, populates SharedDocContext with expose_to_scripts values.
   *
   * @param hits the primary TopDocs (rescore window)
   * @param primarySearcher the primary index searcher (for reading primary doc values)
   * @param sharedDocContext the shared doc context for populating script-accessible values
   */
  public void materialize(
      TopDocs hits, SearcherAndTaxonomy primarySearcher, SharedDocContext sharedDocContext)
      throws IOException {
    if (lookupStates.isEmpty()) {
      return;
    }

    // Phase 1: Collect join keys from primary hits for each lookup (sequential, fast)
    List<PreparedLookup> prepared = new ArrayList<>();
    for (LookupState state : lookupStates.values()) {
      if (state.fieldsToRead.isEmpty()) {
        continue;
      }

      Map<Integer, String> docIdToJoinKey = new HashMap<>();
      Set<String> joinKeys = new HashSet<>();
      collectJoinKeys(hits, primarySearcher, state, docIdToJoinKey, joinKeys);

      int maxKeys = state.config.getMaxKeys();
      if (maxKeys > 0 && joinKeys.size() > maxKeys) {
        throw new IllegalStateException(
            "CrossIndexLookup for index \""
                + state.config.getIndex()
                + "\": "
                + joinKeys.size()
                + " unique join keys exceed max_keys limit of "
                + maxKeys);
      }

      int topHits = state.config.getTopHits() > 0 ? state.config.getTopHits() : DEFAULT_TOP_HITS;
      prepared.add(new PreparedLookup(state, docIdToJoinKey, joinKeys, topHits));
    }

    // Phase 2: Search secondary indices
    if (prepared.size() == 1) {
      // Single lookup: search directly, no parallelization overhead
      PreparedLookup p = prepared.get(0);
      p.state.materializedResults = searchSecondaryIndex(p.state, p.joinKeys, p.topHits);
    } else if (prepared.size() > 1) {
      // Multiple lookups: search in parallel when an executor is available
      if (searchExecutor != null) {
        searchSecondaryIndicesParallel(prepared, searchExecutor);
      } else {
        for (PreparedLookup p : prepared) {
          p.state.materializedResults = searchSecondaryIndex(p.state, p.joinKeys, p.topHits);
        }
      }
    }

    // Phase 3: Populate SharedDocContext sequentially (inner map is not thread-safe)
    for (PreparedLookup p : prepared) {
      if (!p.state.config.getExposeToScriptsList().isEmpty()) {
        populateSharedDocContext(hits, p.state, p.docIdToJoinKey, sharedDocContext);
      }
    }
  }

  /** Intermediate state between join key collection and secondary search. */
  private record PreparedLookup(
      LookupState state, Map<Integer, String> docIdToJoinKey, Set<String> joinKeys, int topHits) {}

  /** Search multiple secondary indices concurrently using the provided executor. */
  private void searchSecondaryIndicesParallel(List<PreparedLookup> prepared, Executor executor)
      throws IOException {
    List<CompletableFuture<Void>> futures = new ArrayList<>(prepared.size());
    for (PreparedLookup p : prepared) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  p.state.materializedResults =
                      searchSecondaryIndex(p.state, p.joinKeys, p.topHits);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              },
              executor));
    }
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UncheckedIOException uio) {
        throw uio.getCause();
      }
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      throw new IOException("Parallel cross-index search failed", cause);
    }
  }

  /** Collect join key values from primary hits by reading primary field doc values. */
  private void collectJoinKeys(
      TopDocs hits,
      SearcherAndTaxonomy primarySearcher,
      LookupState state,
      Map<Integer, String> docIdToJoinKey,
      Set<String> joinKeys)
      throws IOException {
    List<LeafReaderContext> leaves = primarySearcher.searcher().getIndexReader().leaves();
    int leafIdx = 0;
    LoadedDocValues<?> primaryDV = null;

    // Sort by doc ID for efficient segment traversal
    ScoreDoc[] sorted = hits.scoreDocs.clone();
    java.util.Arrays.sort(sorted, java.util.Comparator.comparingInt(d -> d.doc));

    for (ScoreDoc scoreDoc : sorted) {
      // Advance to correct leaf
      while (leafIdx < leaves.size() - 1 && leaves.get(leafIdx + 1).docBase <= scoreDoc.doc) {
        leafIdx++;
        primaryDV = null;
      }

      if (primaryDV == null) {
        primaryDV = state.primaryFieldDef.getDocValues(leaves.get(leafIdx));
      }

      int segmentDocId = scoreDoc.doc - leaves.get(leafIdx).docBase;
      primaryDV.setDocId(segmentDocId);

      String joinKey = null;
      if (primaryDV.size() > 0) {
        joinKey = toCanonicalKey(primaryDV.toFieldValue(0));
      }

      if (joinKey != null && !joinKey.isEmpty()) {
        docIdToJoinKey.put(scoreDoc.doc, joinKey);
        joinKeys.add(joinKey);
      }
    }
  }

  /**
   * Search the secondary index for docs matching join keys, reading field values inline during
   * collection. Per-key results are capped at topHits, and collection terminates early per-slice
   * once all keys are saturated. Uses CollectorManager to leverage parallel segment search when the
   * searcher has an executor.
   */
  private Map<String, List<Map<String, CompositeFieldValue>>> searchSecondaryIndex(
      LookupState state, Set<String> joinKeys, int topHits) throws IOException {
    if (joinKeys.isEmpty()) {
      return new HashMap<>();
    }

    TermQueryable termQueryable = (TermQueryable) state.secondaryFieldDef;

    // Build type-safe query via the field's TermQueryable implementation
    org.apache.lucene.search.Query query =
        termQueryable.getTermInSetQueryFromTextValues(new ArrayList<>(joinKeys));

    // Prepare field def list for inline reading in collector
    List<Map.Entry<String, FieldDef>> fieldEntries = new ArrayList<>(state.fieldsToRead.entrySet());
    int totalKeys = joinKeys.size();

    // Use CollectorManager for parallel segment search
    return state
        .searcher
        .searcher()
        .search(
            query,
            new CollectorManager<
                InlineFieldCollector, Map<String, List<Map<String, CompositeFieldValue>>>>() {
              @Override
              public InlineFieldCollector newCollector() {
                return new InlineFieldCollector(
                    state.secondaryFieldDef, fieldEntries, topHits, totalKeys);
              }

              @Override
              public Map<String, List<Map<String, CompositeFieldValue>>> reduce(
                  Collection<InlineFieldCollector> collectors) {
                Map<String, List<Map<String, CompositeFieldValue>>> merged = new HashMap<>();
                for (InlineFieldCollector collector : collectors) {
                  for (Map.Entry<String, List<Map<String, CompositeFieldValue>>> entry :
                      collector.localResults.entrySet()) {
                    List<Map<String, CompositeFieldValue>> existing =
                        merged.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
                    for (Map<String, CompositeFieldValue> hit : entry.getValue()) {
                      if (existing.size() < topHits) {
                        existing.add(hit);
                      }
                    }
                  }
                }
                return merged;
              }
            });
  }

  /**
   * Collector that reads join field and retrieve/expose field doc values inline during collection.
   * Each instance maintains its own local results map, making it safe for parallel use across
   * segments.
   */
  private static class InlineFieldCollector extends SimpleCollector {
    private final IndexableFieldDef<?> secondaryFieldDef;
    private final List<Map.Entry<String, FieldDef>> fieldEntries;
    private final int topHits;
    private final int totalKeys;

    final Map<String, List<Map<String, CompositeFieldValue>>> localResults = new HashMap<>();

    private LoadedDocValues<?> joinDV;
    private final LoadedDocValues<?>[] fieldDVs;
    private int saturatedKeys = 0;

    InlineFieldCollector(
        IndexableFieldDef<?> secondaryFieldDef,
        List<Map.Entry<String, FieldDef>> fieldEntries,
        int topHits,
        int totalKeys) {
      this.secondaryFieldDef = secondaryFieldDef;
      this.fieldEntries = fieldEntries;
      this.topHits = topHits;
      this.totalKeys = totalKeys;
      this.fieldDVs = new LoadedDocValues<?>[fieldEntries.size()];
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      joinDV = secondaryFieldDef.getDocValues(context);
      for (int i = 0; i < fieldEntries.size(); i++) {
        IndexableFieldDef<?> fd = (IndexableFieldDef<?>) fieldEntries.get(i).getValue();
        fieldDVs[i] = fd.getDocValues(context);
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      joinDV.setDocId(doc);
      if (joinDV.size() == 0) return;
      String key = toCanonicalKey(joinDV.toFieldValue(0));

      List<Map<String, CompositeFieldValue>> keyResults =
          localResults.computeIfAbsent(key, k -> new ArrayList<>());
      if (keyResults.size() >= topHits) {
        return;
      }

      // Read all requested fields inline while DV is warm for this segment
      Map<String, CompositeFieldValue> fieldValues = new LinkedHashMap<>();
      for (int i = 0; i < fieldEntries.size(); i++) {
        fieldDVs[i].setDocId(doc);
        CompositeFieldValue.Builder cfv = CompositeFieldValue.newBuilder();
        for (int j = 0; j < fieldDVs[i].size(); j++) {
          cfv.addFieldValue(fieldDVs[i].toFieldValue(j));
        }
        fieldValues.put(fieldEntries.get(i).getKey(), cfv.build());
      }
      keyResults.add(fieldValues);

      // Early termination per slice: stop once all keys have reached topHits
      if (keyResults.size() == topHits) {
        saturatedKeys++;
        if (saturatedKeys == totalKeys) {
          throw new CollectionTerminatedException();
        }
      }
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
      return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  /** Convert a FieldValue to a canonical string key for join key matching. */
  private static String toCanonicalKey(FieldValue fv) {
    return switch (fv.getFieldValuesCase()) {
      case TEXTVALUE -> fv.getTextValue();
      case INTVALUE -> String.valueOf(fv.getIntValue());
      case LONGVALUE -> String.valueOf(fv.getLongValue());
      default -> fv.getTextValue();
    };
  }

  /** Populate SharedDocContext with expose_to_scripts values for each primary hit. */
  private void populateSharedDocContext(
      TopDocs hits,
      LookupState state,
      Map<Integer, String> docIdToJoinKey,
      SharedDocContext sharedDocContext) {
    String indexName = state.config.getIndex();

    for (ScoreDoc scoreDoc : hits.scoreDocs) {
      String joinKey = docIdToJoinKey.get(scoreDoc.doc);
      if (joinKey == null) continue;

      List<Map<String, CompositeFieldValue>> secondaryHits = state.materializedResults.get(joinKey);
      if (secondaryHits == null || secondaryHits.isEmpty()) continue;

      Map<String, Object> docContext = sharedDocContext.getContext(scoreDoc.doc);

      for (String exposeField : state.config.getExposeToScriptsList()) {
        List<Object> values = new ArrayList<>();
        for (Map<String, CompositeFieldValue> hit : secondaryHits) {
          CompositeFieldValue cfv = hit.get(exposeField);
          if (cfv != null && cfv.getFieldValueCount() > 0) {
            FieldValue fv = cfv.getFieldValue(0);
            values.add(fieldValueToObject(fv));
          }
        }
        docContext.put("cross_" + indexName + "_" + exposeField, values);
      }
    }
  }

  /** Convert a FieldValue proto to a Java object for SharedDocContext. */
  private static Object fieldValueToObject(FieldValue fv) {
    return switch (fv.getFieldValuesCase()) {
      case TEXTVALUE -> fv.getTextValue();
      case BOOLEANVALUE -> fv.getBooleanValue();
      case INTVALUE -> fv.getIntValue();
      case LONGVALUE -> fv.getLongValue();
      case FLOATVALUE -> fv.getFloatValue();
      case DOUBLEVALUE -> fv.getDoubleValue();
      default -> fv.getTextValue();
    };
  }

  @Override
  public void close() {
    for (LookupState state : lookupStates.values()) {
      try {
        state.secondaryShard.release(state.searcher);
      } catch (Exception e) {
        logger.error(
            "CrossIndexLookupManager: failed to release searcher for index \"{}\"",
            state.config.getIndex(),
            e);
      }
    }
  }
}
