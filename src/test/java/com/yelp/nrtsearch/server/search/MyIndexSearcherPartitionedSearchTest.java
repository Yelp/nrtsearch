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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.CompletionQuery;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.LabelAndValue;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.index.ShardState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.Test;

/**
 * Verify that search results are the same whether or not index segments are divided into multiple
 * partitions for parallel search.
 */
public class MyIndexSearcherPartitionedSearchTest extends ServerTestCase {
  private static final int NUM_DOCS = 2000;
  private static final int NUM_COLORS = 5;

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/PartitionedSearchRegisterFields.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
  }

  /** Recreate index contents, which also opens a new searcher using the current live settings. */
  private void reindex() throws Exception {
    ShardState shardState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX).getShard(0);
    IndexWriter writer = shardState.writer;
    writer.deleteAll();
    // one large segment, and one small one
    addDocs(DEFAULT_TEST_INDEX, 0, NUM_DOCS - 100);
    writer.commit();
    addDocs(DEFAULT_TEST_INDEX, NUM_DOCS - 100, NUM_DOCS);
    writer.commit();
    shardState.maybeRefreshBlocking();
  }

  private void addDocs(String name, int start, int end) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = start; i < end; ++i) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields("doc_id", value(String.valueOf(i)))
              .putFields("int_field", value(String.valueOf(i)))
              .putFields("color", value("color_" + (i % NUM_COLORS)))
              .putFields(
                  "suggest",
                  value(
                      "{\"value\":\"item "
                          + i
                          + "\",\"contexts\":[\"c\"],\"weight\":"
                          + (NUM_DOCS - i)
                          + "}"))
              .build());
    }
    addDocuments(docs.stream());
  }

  private static AddDocumentRequest.MultiValuedField value(String v) {
    return AddDocumentRequest.MultiValuedField.newBuilder().addValue(v).build();
  }

  private void setSliceStrategy(MyIndexSearcher.SliceStrategy strategy) throws Exception {
    getGlobalState()
        .getIndexStateManagerOrThrow(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder()
                .setSliceStrategy(StringValue.newBuilder().setValue(strategy.name()).build())
                .setSliceTarget(Int32Value.newBuilder().setValue(4).build())
                .setSliceMinPartitionDocs(Int32Value.newBuilder().setValue(100).build())
                .setSliceSnapTolerance(DoubleValue.newBuilder().setValue(0.1).build())
                .build(),
            false);
    reindex();
  }

  private int numSplitSegments() throws IOException {
    ShardState shardState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX).getShard(0);
    SearcherAndTaxonomy s = shardState.acquire();
    try {
      return MyIndexSearcher.getSliceStats(s.searcher().getSlices()).numSplitSegments();
    } finally {
      shardState.release(s);
    }
  }

  private List<SearchRequest> requests() {
    Query rangeQuery =
        Query.newBuilder()
            .setRangeQuery(
                RangeQuery.newBuilder().setField("int_field").setLower("100").setUpper("1500"))
            .build();
    Query termQuery =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("color").setTextValue("color_2"))
            .build();
    Query completionQuery =
        Query.newBuilder()
            .setCompletionQuery(
                CompletionQuery.newBuilder().setField("suggest").setText("item 1").build())
            .build();
    return List.of(
        // hit count only
        SearchRequest.newBuilder().setIndexName(DEFAULT_TEST_INDEX).setTopHits(0).build(),
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(0)
            .setQuery(rangeQuery)
            .build(),
        // top hits, sorted so that the results do not depend on doc id order
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(20)
            .setTotalHitsThreshold(Integer.MAX_VALUE)
            .setQuery(rangeQuery)
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(
                                SortType.newBuilder().setFieldName("int_field").setReverse(true))))
            .addRetrieveFields("doc_id")
            .build(),
        // facets, uses drill sideways
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(0)
            .setQuery(termQuery)
            .addFacets(Facet.newBuilder().setDim("color").setTopN(10).build())
            .build(),
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(0)
            .addFacets(Facet.newBuilder().setDim("color").setTopN(10).build())
            .build(),
        // completion query
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(20)
            .setQuery(completionQuery)
            .addRetrieveFields("doc_id")
            .build());
  }

  private record Result(long totalHits, List<String> ids, Map<String, Double> facets) {}

  private static Result toResult(SearchResponse response) {
    List<String> ids = new ArrayList<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      ids.add(hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    }
    Map<String, Double> facets = new HashMap<>();
    for (FacetResult facetResult : response.getFacetResultList()) {
      for (LabelAndValue lv : facetResult.getLabelValuesList()) {
        facets.put(facetResult.getDim() + ":" + lv.getLabel(), lv.getValue());
      }
    }
    return new Result(response.getTotalHits().getValue(), ids, facets);
  }

  private List<Result> runRequests() {
    List<Result> results = new ArrayList<>();
    for (SearchRequest request : requests()) {
      results.add(toResult(getGrpcServer().getBlockingStub().search(request)));
    }
    return results;
  }

  @Test
  public void testPartitionedSearchMatchesSegmentSearch() throws Exception {
    setSliceStrategy(MyIndexSearcher.SliceStrategy.SEGMENT);
    assertEquals(0, numSplitSegments());
    List<Result> expected = runRequests();
    assertEquals(NUM_DOCS, expected.get(0).totalHits());
    assertEquals(1401, expected.get(1).totalHits());
    assertEquals("1500", expected.get(2).ids().getFirst());
    assertEquals(20, expected.get(2).ids().size());
    assertEquals(NUM_DOCS / NUM_COLORS, expected.get(3).totalHits());
    assertEquals(NUM_COLORS, expected.get(4).facets().size());
    for (double count : expected.get(4).facets().values()) {
      assertEquals(NUM_DOCS / NUM_COLORS, count, 0.0);
    }
    assertTrue(expected.get(5).totalHits() > 0);

    try {
      for (MyIndexSearcher.SliceStrategy strategy :
          List.of(
              MyIndexSearcher.SliceStrategy.BALANCED_CUT,
              MyIndexSearcher.SliceStrategy.SPLIT_OUTLIERS)) {
        setSliceStrategy(strategy);
        assertTrue(numSplitSegments() > 0);
        List<Result> actual = runRequests();
        for (int i = 0; i < expected.size(); ++i) {
          assertEquals(strategy + " request " + i, expected.get(i), actual.get(i));
        }
      }
    } finally {
      setSliceStrategy(MyIndexSearcher.SliceStrategy.SEGMENT);
    }
  }
}
