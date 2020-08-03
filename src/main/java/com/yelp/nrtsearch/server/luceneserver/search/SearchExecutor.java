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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;

/**
 * Class to handle execution of a search query. Execution is broken into two phases, query and
 * fetch. The query phase produces a {@link SearchResponse.Builder} containing the query hits
 * populated with their lucene doc ids and scores. The fetch phase fills in the other field data
 * needed for each hit.
 */
public class SearchExecutor {

  private SearchExecutor() {}

  /**
   * Execute the query phase of a search. From the given {@link SearchContext} information, perform
   * the required queries and construct a {@link SearchResponse.Builder} populated with the
   * resulting hits. The hits will only contain the lucene doc ids and scores.
   *
   * @param context search context
   * @return response builder containing query hits
   * @throws IOException on search error
   */
  public static SearchResponse.Builder doQueryPhase(SearchContext context) throws IOException {
    SearchResponse.Builder searchResponse = SearchResponse.newBuilder();

    long searchStartTime = System.nanoTime();
    TopDocs hits;
    try {
      hits =
          context
              .searcherAndTaxonomy()
              .searcher
              .search(context.query(), context.collector().getManager());
    } catch (TimeLimitingCollector.TimeExceededException tee) {
      searchResponse.setHitTimeout(true);
      return searchResponse;
    }
    context
        .diagnostics()
        .setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

    hits = getHitsFromOffset(hits, context.startHit());
    setResponseHits(searchResponse, hits);

    SearchResponse.SearchState.Builder searchState = SearchResponse.SearchState.newBuilder();
    searchState.setTimestamp(context.timestampSec());
    searchState.setSearcherVersion(
        ((DirectoryReader) context.searcherAndTaxonomy().searcher.getIndexReader()).getVersion());
    if (hits.scoreDocs.length != 0) {
      ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
      searchState.setLastDocId(lastHit.doc);
      context.collector().fillLastHit(searchState, lastHit);
    }
    searchResponse.setSearchState(searchState);

    return searchResponse;
  }

  /**
   * Given all the top documents and a starting offset, produce a slice of the documents starting
   * from that offset.
   *
   * @param hits all hits
   * @param startHit offset into top docs
   * @return slice of hits starting at given offset, or empty slice if there are less startHit docs
   */
  private static TopDocs getHitsFromOffset(TopDocs hits, int startHit) {
    if (startHit != 0) {
      // Slice:
      int count = Math.max(0, hits.scoreDocs.length - startHit);
      ScoreDoc[] newScoreDocs = new ScoreDoc[count];
      if (count > 0) {
        System.arraycopy(hits.scoreDocs, startHit, newScoreDocs, 0, count);
      }
      return new TopDocs(hits.totalHits, newScoreDocs);
    }
    return hits;
  }

  /**
   * Add {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder}s to the {@link
   * SearchResponse.Builder} for each of the query hits. Populate the builders with the lucene doc
   * id and score.
   *
   * @param searchResponse search response to add hits
   * @param hits hits from query
   */
  private static void setResponseHits(SearchResponse.Builder searchResponse, TopDocs hits) {
    TotalHits totalHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(hits.totalHits.relation.name()))
            .setValue(hits.totalHits.value)
            .build();
    searchResponse.setTotalHits(totalHits);
    for (int hitIndex = 0; hitIndex < hits.scoreDocs.length; hitIndex++) {
      ScoreDoc hit = hits.scoreDocs[hitIndex];
      var hitResponse = searchResponse.addHitsBuilder();
      hitResponse.setLuceneDocId(hit.doc);
      if (!Float.isNaN(hit.score)) {
        hitResponse.setScore(hit.score);
      }
    }
  }

  /**
   * Execute the fetch phase for this query. Fills hit field information for the supplied {@link
   * SearchResponse}. The response is expected to already contain {@link
   * com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder} entries with the lucene doc ids and
   * scores set.
   *
   * @param context search context
   * @param searchResponse response to fill field data for
   * @throws IOException on error reading field data
   */
  public static void doFetchPhase(SearchContext context, SearchResponse.Builder searchResponse)
      throws IOException {
    if (searchResponse.getHitsCount() == 0) {
      return;
    }
    long t0 = System.nanoTime();

    List<SearchResponse.Hit.Builder> sortedHits =
        new ArrayList<>(searchResponse.getHitsBuilderList());
    sortedHits.sort(Comparator.comparingInt(SearchResponse.Hit.Builder::getLuceneDocId));

    List<LeafReaderContext> leaves =
        context.searcherAndTaxonomy().searcher.getIndexReader().leaves();
    int hitIndex = 0;
    int leafIndex = ReaderUtil.subIndex(sortedHits.get(0).getLuceneDocId(), leaves);
    while (hitIndex < sortedHits.size()) {
      leafIndex =
          getSegmentIndexForDocId(leaves, leafIndex, sortedHits.get(hitIndex).getLuceneDocId());
      LeafReaderContext sliceSegment = leaves.get(leafIndex);
      List<SearchResponse.Hit.Builder> sliceHits = getSliceHits(sortedHits, hitIndex, sliceSegment);

      fetchSlice(context, sliceHits, sliceSegment);

      hitIndex += sliceHits.size();
    }
    context.diagnostics().setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000.0));
  }

  /**
   * Get the index of the lucene segment containing the given doc id. The search will start at the
   * provided index and walk through the segment list until found.
   *
   * @param contexts list of segment contexts for query
   * @param nextSegmentIndex index of segment to start searching
   * @param docId lucene doc id to find
   * @return index of segment containing docId
   * @throws IllegalArgumentException if the starting segment is already past the docId, or if doc
   *     is not in any segment.
   */
  private static int getSegmentIndexForDocId(
      List<LeafReaderContext> contexts, int nextSegmentIndex, int docId) {
    if (docId < contexts.get(nextSegmentIndex).docBase) {
      throw new IllegalArgumentException(
          "docId: "
              + docId
              + " is below initial docBase: "
              + contexts.get(nextSegmentIndex).docBase);
    }

    int segmentIndex = nextSegmentIndex;
    while (segmentIndex < contexts.size()) {
      LeafReaderContext context = contexts.get(segmentIndex);
      int endDoc = context.docBase + context.reader().maxDoc();
      if (docId >= endDoc) {
        segmentIndex++;
        continue;
      }
      if (docId == context.docBase) {
        if ((segmentIndex + 1) < contexts.size()
            && contexts.get(segmentIndex + 1).docBase == docId) {
          segmentIndex++;
          continue;
        }
      }
      return segmentIndex;
    }
    throw new IllegalArgumentException("Unable to find Segment context for docId: " + docId);
  }

  private static List<SearchResponse.Hit.Builder> getSliceHits(
      List<SearchResponse.Hit.Builder> sortedHits, int startIndex, LeafReaderContext sliceSegment) {
    int endDoc = sliceSegment.docBase + sliceSegment.reader().maxDoc();
    int endIndex = startIndex + 1;
    while (endIndex < sortedHits.size()) {
      if (sortedHits.get(endIndex).getLuceneDocId() >= endDoc) {
        break;
      }
      endIndex++;
    }
    return sortedHits.subList(startIndex, endIndex);
  }

  /**
   * Fetch all the required field data For a slice of hits. All these hit reside in the same lucene
   * segment.
   *
   * @param context search context
   * @param sliceHits hits in this slice
   * @param sliceSegment lucene segment context for slice
   * @throws IOException on issue reading document data
   */
  private static void fetchSlice(
      SearchContext context,
      List<SearchResponse.Hit.Builder> sliceHits,
      LeafReaderContext sliceSegment)
      throws IOException {
    for (Map.Entry<String, FieldDef> fieldDefEntry : context.queryFields().entrySet()) {
      if (fieldDefEntry.getValue() instanceof VirtualFieldDef) {
        fetchFromValueSource(
            context,
            sliceHits,
            sliceSegment,
            fieldDefEntry.getKey(),
            (VirtualFieldDef) fieldDefEntry.getValue());
      } else if (fieldDefEntry.getValue() instanceof IndexableFieldDef) {
        IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDefEntry.getValue();
        if (indexableFieldDef.hasDocValues()) {
          fetchFromDocVales(
              context, sliceHits, sliceSegment, fieldDefEntry.getKey(), indexableFieldDef);
        } else if (indexableFieldDef.isStored()) {
          fetchFromStored(context, sliceHits, fieldDefEntry.getKey(), indexableFieldDef);
        } else {
          throw new IllegalStateException(
              "No valid method to retrieve indexable field: " + fieldDefEntry.getKey());
        }
      } else {
        throw new IllegalStateException(
            "No valid method to retrieve field: " + fieldDefEntry.getKey());
      }
    }
    addSpecialSortFields(sliceHits, context.sortFieldNames());
  }

  /** Fetch field value from virtual field's {@link org.apache.lucene.search.DoubleValuesSource} */
  private static void fetchFromValueSource(
      SearchContext context,
      List<SearchResponse.Hit.Builder> sliceHits,
      LeafReaderContext sliceSegment,
      String name,
      VirtualFieldDef virtualFieldDef)
      throws IOException {
    SettableScoreDoubleValues scoreValue = new SettableScoreDoubleValues();
    DoubleValues doubleValues =
        virtualFieldDef.getValuesSource().getValues(sliceSegment, scoreValue);
    for (SearchResponse.Hit.Builder hit : sliceHits) {
      int docID = hit.getLuceneDocId() - sliceSegment.docBase;
      scoreValue.setScore(hit.getScore());
      doubleValues.advanceExact(docID);

      SearchResponse.Hit.CompositeFieldValue.Builder compositeFieldValue =
          SearchResponse.Hit.CompositeFieldValue.newBuilder();
      compositeFieldValue.addFieldValue(
          SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(doubleValues.doubleValue()));
      setFieldValueForHit(name, compositeFieldValue.build(), hit, context);
    }
  }

  /** Fetch field value from its doc value */
  private static void fetchFromDocVales(
      SearchContext context,
      List<SearchResponse.Hit.Builder> sliceHits,
      LeafReaderContext sliceSegment,
      String name,
      IndexableFieldDef indexableFieldDef)
      throws IOException {
    LoadedDocValues<?> docValues = indexableFieldDef.getDocValues(sliceSegment);
    for (SearchResponse.Hit.Builder hit : sliceHits) {
      int docID = hit.getLuceneDocId() - sliceSegment.docBase;
      docValues.setDocId(docID);

      SearchResponse.Hit.CompositeFieldValue.Builder compositeFieldValue =
          SearchResponse.Hit.CompositeFieldValue.newBuilder();
      for (int i = 0; i < docValues.size(); ++i) {
        compositeFieldValue.addFieldValue(docValues.toFieldValue(i));
      }
      setFieldValueForHit(name, compositeFieldValue.build(), hit, context);
    }
  }

  /** Fetch field value stored in the index */
  private static void fetchFromStored(
      SearchContext context,
      List<SearchResponse.Hit.Builder> sliceHits,
      String name,
      IndexableFieldDef indexableFieldDef)
      throws IOException {
    for (SearchResponse.Hit.Builder hit : sliceHits) {
      String[] values =
          indexableFieldDef.getStored(
              context.searcherAndTaxonomy().searcher.doc(hit.getLuceneDocId()));

      SearchResponse.Hit.CompositeFieldValue.Builder compositeFieldValue =
          SearchResponse.Hit.CompositeFieldValue.newBuilder();
      for (String fieldValue : values) {
        compositeFieldValue.addFieldValue(
            SearchResponse.Hit.FieldValue.newBuilder().setTextValue(fieldValue));
      }
      setFieldValueForHit(name, compositeFieldValue.build(), hit, context);
    }
  }

  /**
   * Add the given {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue}
   * into the response hit. It may be needed in sort fields, retrieve fields, or both.
   *
   * @param name field name
   * @param fieldValue loaded field value0
   * @param hit response hit
   * @param context search context
   */
  private static void setFieldValueForHit(
      String name,
      SearchResponse.Hit.CompositeFieldValue fieldValue,
      SearchResponse.Hit.Builder hit,
      SearchContext context) {
    if (context.sortFieldNames().contains(name)) {
      hit.putSortedFields(name, fieldValue);
      if (context.retrieveFieldNames().contains(name)) {
        hit.putFields(name, fieldValue);
      }
    } else {
      hit.putFields(name, fieldValue);
    }
  }

  /**
   * Add sort values for fields that are not index fields, into {@link SearchResponse.Hit.Builder}s.
   * These would be special sort fields like 'docid' and 'score'.
   *
   * @param sliceHits segment hits
   * @param sortFieldNames names of query sort field names
   * @throws IllegalArgumentException if special field name is unknown
   */
  private static void addSpecialSortFields(
      List<SearchResponse.Hit.Builder> sliceHits, Set<String> sortFieldNames) {
    if (sortFieldNames.isEmpty()) {
      return;
    }
    for (String specialField : SortParser.SPECIAL_FIELDS) {
      if (sortFieldNames.contains(specialField)) {
        if (specialField.equals("docid")) {
          for (SearchResponse.Hit.Builder hit : sliceHits) {
            var fieldValue =
                SearchResponse.Hit.CompositeFieldValue.newBuilder()
                    .addFieldValue(
                        SearchResponse.Hit.FieldValue.newBuilder()
                            .setIntValue(hit.getLuceneDocId())
                            .build())
                    .build();
            hit.putFields(specialField, fieldValue);
          }
        } else if (specialField.equals("score")) {
          for (SearchResponse.Hit.Builder hit : sliceHits) {
            var fieldValue =
                SearchResponse.Hit.CompositeFieldValue.newBuilder()
                    .addFieldValue(
                        SearchResponse.Hit.FieldValue.newBuilder()
                            .setDoubleValue(hit.getScore())
                            .build())
                    .build();
            hit.putFields(specialField, fieldValue);
          }
        } else {
          throw new IllegalArgumentException("Unknown special sort field: " + specialField);
        }
      }
    }
  }

  /**
   * {@link DoubleValues} implementation used to inject the document score into the execution of
   * scripts to populate virtual fields.
   */
  private static class SettableScoreDoubleValues extends DoubleValues {
    private double score = Double.NaN;

    @Override
    public double doubleValue() {
      return score;
    }

    @Override
    public boolean advanceExact(int doc) {
      return !Double.isNaN(score);
    }

    public void setScore(double score) {
      this.score = score;
    }
  }
}
