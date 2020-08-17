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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DoubleValues;

/** Class for executing the fetch phase of a search query. */
public class FetchOperation {

  private FetchOperation() {}

  /**
   * Execute the fetch phase for this query. Fills hit field information for the context {@link
   * SearchResponse}. The response is expected to already contain {@link
   * com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder} entries with the lucene doc ids and
   * scores/sort set.
   *
   * @param context search context
   * @throws IOException on error reading field data
   */
  public static void execute(SearchContext context) throws IOException {
    if (context.searchResponse().getHitsCount() == 0) {
      return;
    }
    long t0 = System.nanoTime();

    // sort hits by lucene doc id
    List<SearchResponse.Hit.Builder> sortedHits =
        new ArrayList<>(context.searchResponse().getHitsBuilderList());
    sortedHits.sort(Comparator.comparingInt(SearchResponse.Hit.Builder::getLuceneDocId));

    List<LeafReaderContext> leaves =
        context.searcherAndTaxonomy().searcher.getIndexReader().leaves();
    int hitIndex = 0;
    int leafIndex = ReaderUtil.subIndex(sortedHits.get(0).getLuceneDocId(), leaves);
    while (hitIndex < sortedHits.size()) {
      leafIndex =
          getSegmentIndexForDocId(leaves, leafIndex, sortedHits.get(hitIndex).getLuceneDocId());
      LeafReaderContext sliceSegment = leaves.get(leafIndex);

      // get all hits in the same segment and process them together for better resource reuse
      List<SearchResponse.Hit.Builder> sliceHits = getSliceHits(sortedHits, hitIndex, sliceSegment);
      fetchSlice(context, sliceHits, sliceSegment);

      hitIndex += sliceHits.size();
      leafIndex++;
    }
    context
        .searchResponse()
        .getDiagnosticsBuilder()
        .setGetFieldsTimeMs(((System.nanoTime() - t0) / 1000000.0));
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
    if (nextSegmentIndex < contexts.size() && docId < contexts.get(nextSegmentIndex).docBase) {
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

  /** Get all hits belonging to the same lucene segment */
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
    for (Map.Entry<String, FieldDef> fieldDefEntry : context.retrieveFields().entrySet()) {
      if (fieldDefEntry.getValue() instanceof VirtualFieldDef) {
        fetchFromValueSource(
            sliceHits,
            sliceSegment,
            fieldDefEntry.getKey(),
            (VirtualFieldDef) fieldDefEntry.getValue());
      } else if (fieldDefEntry.getValue() instanceof IndexableFieldDef) {
        IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDefEntry.getValue();
        if (indexableFieldDef.hasDocValues()) {
          fetchFromDocVales(sliceHits, sliceSegment, fieldDefEntry.getKey(), indexableFieldDef);
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
  }

  /** Fetch field value from virtual field's {@link org.apache.lucene.search.DoubleValuesSource} */
  private static void fetchFromValueSource(
      List<SearchResponse.Hit.Builder> sliceHits,
      LeafReaderContext sliceSegment,
      String name,
      VirtualFieldDef virtualFieldDef)
      throws IOException {
    FetchOperation.SettableScoreDoubleValues scoreValue =
        new FetchOperation.SettableScoreDoubleValues();
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
      hit.putFields(name, compositeFieldValue.build());
    }
  }

  /** Fetch field value from its doc value */
  private static void fetchFromDocVales(
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
      hit.putFields(name, compositeFieldValue.build());
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
      hit.putFields(name, compositeFieldValue.build());
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
