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
package com.yelp.nrtsearch.server.luceneserver.facet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.NumericRangeType;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class FacetTopHitsTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/facet_top_hits.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (int id = 0; id < NUM_DOCS; ++id) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf((id + 25) % NUM_DOCS))
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id / 10))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  @Test
  public void testFacetTopHits() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    SearchResponse response = doQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(20)
            .build();
    response = doQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        9,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("6")), 4));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(31)
            .setTopN(20)
            .build();
    response = doQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        31,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("6", "5")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(66)
            .setTopN(20)
            .build();
    response = doQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        66,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 1));
  }

  @Test
  public void testFacetTopHitsSortField() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    SearchResponse response = doSortQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        9,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("6")), 4));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(31)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        31,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("6", "5")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(66)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 100);
    assertEquals(100, response.getHitsCount());
    assertResponse(
        response,
        66,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 1));
  }

  @Test
  public void testLessTopHits() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    SearchResponse response = doQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(20)
            .build();
    response = doQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        9,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("6")), 4));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(31)
            .setTopN(20)
            .build();
    response = doQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        31,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("6", "5")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(66)
            .setTopN(20)
            .build();
    response = doQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        66,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 1));
  }

  @Test
  public void testLessTopHitsSortField() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    SearchResponse response = doSortQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        9,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("6")), 4));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(31)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        31,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("6", "5")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(66)
            .setTopN(20)
            .build();
    response = doSortQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response,
        66,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 1));
  }

  @Test
  public void testTopNFacets() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(1)
            .build();
    SearchResponse response = doQuery(facet, 10);
    assertEquals(10, response.getHitsCount());
    assertResponse(
        response, 9, 2, 1, new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testFieldNotIndexable() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("virtual_field")
            .setSampleTopDocs(9)
            .setTopN(1)
            .build();
    doQuery(facet, 10);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testFieldNoDocValues() {
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet")
            .setDim("no_doc_values")
            .setSampleTopDocs(9)
            .setTopN(1)
            .build();
    doQuery(facet, 10);
  }

  @Test
  public void testMultiFacets() {
    List<Facet> facetList = new ArrayList<>();
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet_100")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    facetList.add(facet);

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet_9")
            .setDim("long_field")
            .setSampleTopDocs(9)
            .setTopN(20)
            .build();
    facetList.add(facet);

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet_31")
            .setDim("long_field")
            .setSampleTopDocs(31)
            .setTopN(20)
            .build();
    facetList.add(facet);

    facet =
        Facet.newBuilder()
            .setName("top_hits_facet_66")
            .setDim("long_field")
            .setSampleTopDocs(66)
            .setTopN(20)
            .build();
    facetList.add(facet);

    SearchResponse response = doQueryMulti(facetList, 10);
    assertEquals(facetList.size(), response.getFacetResultCount());

    assertFacetResult(
        response.getFacetResult(0),
        "top_hits_facet_100",
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));
    assertFacetResult(
        response.getFacetResult(1),
        "top_hits_facet_9",
        9,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("6")), 4));
    assertFacetResult(
        response.getFacetResult(2),
        "top_hits_facet_31",
        31,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("6", "5")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("4")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5));
    assertFacetResult(
        response.getFacetResult(3),
        "top_hits_facet_66",
        66,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6")), 10),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 5),
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 1));
  }

  @Test
  public void testWithNonTopDocsFacet() {
    List<Facet> facetList = new ArrayList<>();
    Facet facet =
        Facet.newBuilder()
            .setName("top_hits_facet_100")
            .setDim("long_field")
            .setSampleTopDocs(100)
            .setTopN(20)
            .build();
    facetList.add(facet);

    List<NumericRangeType> numericRangeTypes = new ArrayList<>();
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("0-7")
            .setMin(0L)
            .setMinInclusive(true)
            .setMax(7L)
            .setMaxInclusive(true)
            .build());
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("8-9")
            .setMin(8L)
            .setMinInclusive(true)
            .setMax(9L)
            .setMaxInclusive(true)
            .build());

    facet =
        Facet.newBuilder()
            .setName("numeric_range")
            .setDim("long_field")
            .setUseOrdsCache(false)
            .addAllNumericRange(numericRangeTypes)
            .setTopN(20)
            .build();
    facetList.add(facet);

    SearchResponse response = doQueryMulti(facetList, 10);
    assertEquals(facetList.size(), response.getFacetResultCount());

    FacetResult facetResult = response.getFacetResult(0);
    assertEquals("numeric_range", facetResult.getName());
    assertEquals(100, facetResult.getValue(), 0);
    assertEquals(2, facetResult.getChildCount());
    assertEquals(2, facetResult.getLabelValuesCount());
    assertEquals("0-7", facetResult.getLabelValues(0).getLabel());
    assertEquals(80.0, facetResult.getLabelValues(0).getValue(), 0);
    assertEquals("8-9", facetResult.getLabelValues(1).getLabel());
    assertEquals(20.0, facetResult.getLabelValues(1).getValue(), 0);

    assertFacetResult(
        response.getFacetResult(1),
        "top_hits_facet_100",
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));
  }

  private static class ExpectedValues {
    public Set<String> labels;
    public double count;

    public ExpectedValues(Set<String> labels, double count) {
      this.labels = labels;
      this.count = count;
    }
  }

  private void assertResponse(
      SearchResponse response,
      double value,
      int childCount,
      int valuesCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getFacetResultCount());
    FacetResult result = response.getFacetResult(0);
    assertTrue(response.getDiagnostics().containsFacetTimeMs("top_hits_facet"));
    assertFacetResult(result, "top_hits_facet", value, childCount, valuesCount, expectedValues);
  }

  private void assertFacetResult(
      FacetResult result,
      String name,
      double value,
      int childCount,
      int valuesCount,
      ExpectedValues... expectedValues) {
    assertEquals(name, result.getName());
    assertEquals(value, result.getValue(), 0);
    assertEquals(childCount, result.getChildCount());
    assertEquals(valuesCount, result.getLabelValuesCount());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, valuesCount);

    int valuesIndex = 0;
    for (ExpectedValues v : expectedValues) {
      Set<String> valueSet = new HashSet<>();
      for (int i = 0; i < v.labels.size(); ++i) {
        valueSet.add(result.getLabelValues(valuesIndex).getLabel());
        assertEquals(v.count, result.getLabelValues(valuesIndex).getValue(), 0);
        valuesIndex++;
      }
      assertEquals(v.labels, valueSet);
    }
  }

  private SearchResponse doQuery(Facet facet, int hits) {
    return doQueryMulti(Collections.singletonList(facet), hits);
  }

  private SearchResponse doQueryMulti(Iterable<Facet> facets, int hits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(hits)
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource("int_field")
                                        .build())
                                .setQuery(
                                    Query.newBuilder()
                                        .setRangeQuery(
                                            RangeQuery.newBuilder()
                                                .setField("int_field")
                                                .setUpper("100")
                                                .setLower("0")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .addRetrieveFields("doc_id")
                .addAllFacets(facets)
                .build());
  }

  private SearchResponse doSortQuery(Facet facet, int hits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(hits)
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setUpper("100")
                                .setLower("0")
                                .build())
                        .build())
                .setQuerySort(
                    QuerySortField.newBuilder()
                        .setFields(
                            SortFields.newBuilder()
                                .addSortedFields(
                                    SortType.newBuilder()
                                        .setFieldName("int_field")
                                        .setReverse(true)
                                        .build())
                                .build())
                        .build())
                .addRetrieveFields("doc_id")
                .addFacets(facet)
                .build());
  }
}
