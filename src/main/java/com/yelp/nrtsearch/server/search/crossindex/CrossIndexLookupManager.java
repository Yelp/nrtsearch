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
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.grpc.CrossIndexLookup;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreDoc;
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

  /** Internal state for a single cross-index lookup. */
  private static class LookupState {
    final CrossIndexLookup config;
    final IndexState secondaryIndex;
    final ShardState secondaryShard;
    final SearcherAndTaxonomy searcher;
    final IndexableFieldDef<?> primaryFieldDef;
    final FieldDef secondaryFieldDef;
    final Map<String, FieldDef> fieldsToRead; // union of retrieve_fields + expose_to_scripts
    // Materialized results: join key -> list of secondary hit field values
    Map<String, List<Map<String, CompositeFieldValue>>> materializedResults;

    LookupState(
        CrossIndexLookup config,
        IndexState secondaryIndex,
        ShardState secondaryShard,
        SearcherAndTaxonomy searcher,
        IndexableFieldDef<?> primaryFieldDef,
        FieldDef secondaryFieldDef,
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

  private CrossIndexLookupManager(Map<String, LookupState> lookupStates) {
    this.lookupStates = lookupStates;
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

    Map<String, LookupState> states = new LinkedHashMap<>();
    for (CrossIndexLookup lookup : lookups) {
      String indexName = lookup.getIndex();
      if (indexName.isEmpty()) {
        throw new IllegalArgumentException("CrossIndexLookup.index must not be empty");
      }
      if (states.containsKey(indexName)) {
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

      // Validate primary field has doc values
      FieldDef primaryFieldDefRaw =
          primaryIndex.docLookup.getFieldDefOrThrow(lookup.getPrimaryField());
      if (!(primaryFieldDefRaw instanceof IndexableFieldDef<?> primaryFieldDef)
          || !primaryFieldDef.hasDocValues()) {
        throw new IllegalArgumentException(
            "CrossIndexLookup requires primary_field \""
                + lookup.getPrimaryField()
                + "\" to have doc values enabled");
      }

      // Resolve secondary index
      IndexState secondaryIndex;
      try {
        secondaryIndex = globalState.getIndexOrThrow(indexName);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "CrossIndexLookup: secondary index \"" + indexName + "\" not found", e);
      }

      FieldDef secondaryFieldDef =
          secondaryIndex.docLookup.getFieldDefOrThrow(lookup.getSecondaryField());

      // Collect all fields to read (union of retrieve_fields + expose_to_scripts)
      Map<String, FieldDef> fieldsToRead = new LinkedHashMap<>();
      for (String field : lookup.getRetrieveFieldsList()) {
        fieldsToRead.put(field, secondaryIndex.docLookup.getFieldDefOrThrow(field));
      }
      for (String field : lookup.getExposeToScriptsList()) {
        fieldsToRead.putIfAbsent(field, secondaryIndex.docLookup.getFieldDefOrThrow(field));
      }

      // Acquire searcher
      ShardState secondaryShard = secondaryIndex.getShard(0);
      SearcherAndTaxonomy searcher;
      try {
        searcher = secondaryShard.acquire();
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
            "CrossIndexLookup: failed to acquire searcher for index \"" + indexName + "\"", e);
      }

      states.put(
          indexName,
          new LookupState(
              lookup,
              secondaryIndex,
              secondaryShard,
              searcher,
              (IndexableFieldDef<?>) primaryFieldDefRaw,
              secondaryFieldDef,
              fieldsToRead));
    }

    return new CrossIndexLookupManager(states);
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
   * from primary doc values, then scans secondary index doc values to find matching documents and
   * reads their field values.
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

    for (LookupState state : lookupStates.values()) {
      if (state.fieldsToRead.isEmpty()) {
        continue;
      }

      // Step 1: Collect join keys from primary hits
      Map<Integer, String> docIdToJoinKey = new HashMap<>();
      Set<String> joinKeys = new HashSet<>();
      collectJoinKeys(hits, primarySearcher, state, docIdToJoinKey, joinKeys);

      // Check max_keys guard
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

      // Step 2: Scan secondary index to find matching docs and read fields
      int topHits = state.config.getTopHits() > 0 ? state.config.getTopHits() : DEFAULT_TOP_HITS;
      state.materializedResults = scanSecondaryIndex(state, joinKeys, topHits);

      // Step 3: Populate SharedDocContext for expose_to_scripts
      if (!state.config.getExposeToScriptsList().isEmpty()) {
        populateSharedDocContext(hits, state, docIdToJoinKey, sharedDocContext);
      }
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
        joinKey = primaryDV.toFieldValue(0).getTextValue();
        if (joinKey.isEmpty()) {
          // For numeric fields, try other value types
          FieldValue fv = primaryDV.toFieldValue(0);
          if (fv.hasIntValue()) {
            joinKey = String.valueOf(fv.getIntValue());
          } else if (fv.hasLongValue()) {
            joinKey = String.valueOf(fv.getLongValue());
          }
        }
      }

      if (joinKey != null && !joinKey.isEmpty()) {
        docIdToJoinKey.put(scoreDoc.doc, joinKey);
        joinKeys.add(joinKey);
      }
    }
  }

  /**
   * Scan secondary index doc values to find docs matching the join keys, then read field values.
   */
  private Map<String, List<Map<String, CompositeFieldValue>>> scanSecondaryIndex(
      LookupState state, Set<String> joinKeys, int topHits) throws IOException {
    Map<String, List<Map<String, CompositeFieldValue>>> results = new HashMap<>();
    if (joinKeys.isEmpty()) {
      return results;
    }

    List<LeafReaderContext> leaves = state.searcher.searcher().getIndexReader().leaves();

    for (LeafReaderContext leaf : leaves) {
      int maxDoc = leaf.reader().maxDoc();
      String secondaryField = state.config.getSecondaryField();
      FieldDef secondaryFieldDef = state.secondaryFieldDef;

      // Determine doc values type and iterate
      if (secondaryFieldDef instanceof IndexableFieldDef<?> indexable) {
        DocValuesType dvType = indexable.getDocValuesType();

        if (dvType == DocValuesType.SORTED) {
          SortedDocValues sdv = leaf.reader().getSortedDocValues(secondaryField);
          if (sdv == null) continue;
          for (int doc = 0; doc < maxDoc; doc++) {
            if (sdv.advanceExact(doc)) {
              String val = sdv.lookupOrd(sdv.ordValue()).utf8ToString();
              if (joinKeys.contains(val)) {
                addSecondaryHit(results, val, leaf, doc, state, topHits);
              }
            }
          }
        } else if (dvType == DocValuesType.SORTED_SET) {
          SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(secondaryField);
          if (ssdv == null) continue;
          for (int doc = 0; doc < maxDoc; doc++) {
            if (ssdv.advanceExact(doc)) {
              boolean matched = false;
              for (int i = 0; i < ssdv.docValueCount() && !matched; i++) {
                String val = ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString();
                if (joinKeys.contains(val)) {
                  addSecondaryHit(results, val, leaf, doc, state, topHits);
                  matched = true;
                }
              }
            }
          }
        } else if (dvType == DocValuesType.NUMERIC) {
          NumericDocValues ndv = leaf.reader().getNumericDocValues(secondaryField);
          if (ndv == null) continue;
          for (int doc = 0; doc < maxDoc; doc++) {
            if (ndv.advanceExact(doc)) {
              String val = Long.toString(ndv.longValue());
              if (joinKeys.contains(val)) {
                addSecondaryHit(results, val, leaf, doc, state, topHits);
              }
            }
          }
        } else if (dvType == DocValuesType.SORTED_NUMERIC) {
          SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues(secondaryField);
          if (sndv == null) continue;
          for (int doc = 0; doc < maxDoc; doc++) {
            if (sndv.advanceExact(doc)) {
              for (int i = 0; i < sndv.docValueCount(); i++) {
                String val = Long.toString(sndv.nextValue());
                if (joinKeys.contains(val)) {
                  addSecondaryHit(results, val, leaf, doc, state, topHits);
                  break;
                }
              }
            }
          }
        }
      }
    }

    return results;
  }

  /** Add a secondary hit's field values to the results map. */
  private void addSecondaryHit(
      Map<String, List<Map<String, CompositeFieldValue>>> results,
      String joinKey,
      LeafReaderContext leaf,
      int segmentDocId,
      LookupState state,
      int topHits)
      throws IOException {
    List<Map<String, CompositeFieldValue>> hitList =
        results.computeIfAbsent(joinKey, k -> new ArrayList<>());
    if (hitList.size() >= topHits) {
      return; // already have enough hits for this key
    }

    Map<String, CompositeFieldValue> fieldValues = new LinkedHashMap<>();
    for (Map.Entry<String, FieldDef> entry : state.fieldsToRead.entrySet()) {
      String fieldName = entry.getKey();
      FieldDef fieldDef = entry.getValue();

      if (fieldDef instanceof IndexableFieldDef<?> indexableDef && indexableDef.hasDocValues()) {
        LoadedDocValues<?> docValues = indexableDef.getDocValues(leaf);
        docValues.setDocId(segmentDocId);
        CompositeFieldValue.Builder cfv = CompositeFieldValue.newBuilder();
        for (int i = 0; i < docValues.size(); i++) {
          cfv.addFieldValue(docValues.toFieldValue(i));
        }
        fieldValues.put(fieldName, cfv.build());
      }
    }

    hitList.add(fieldValues);
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
