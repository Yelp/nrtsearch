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
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
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

public class DocValuesFacetsTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/facet_doc_values.json");
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
              .putFields(
                  "multi_long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id / 11))
                      .addValue(String.valueOf(id / 12))
                      .build())
              .putFields(
                  "atom_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 9))
                      .build())
              .putFields(
                  "multi_atom_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 8))
                      .addValue(String.valueOf(id % 7))
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
  public void testDocValuesFacet() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("long_field").setTopN(10).build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")), 10));
  }

  @Test
  public void testMultiDocValuesFacet() {
    Facet facet =
        Facet.newBuilder()
            .setName("doc_values_facet")
            .setDim("multi_long_field")
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        10,
        10,
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7")), 23),
        new ExpectedValues(new HashSet<>(Collections.singletonList("8")), 15),
        new ExpectedValues(new HashSet<>(Collections.singletonList("9")), 1));
  }

  @Test
  public void testRangeDocValuesFacet() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("long_field").setTopN(10).build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 4));
  }

  @Test
  public void testRangeMultiDocValuesFacet() {
    Facet facet =
        Facet.newBuilder()
            .setName("doc_values_facet")
            .setDim("multi_long_field")
            .setTopN(10)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        2,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 14),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 6));
  }

  @Test
  public void testAtomDocValuesFacet() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("atom_field").setTopN(10).build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        9,
        9,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 12),
        new ExpectedValues(
            new HashSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8")), 11));
  }

  @Test
  public void testAtomMultiDocValuesFacet() {
    Facet facet =
        Facet.newBuilder()
            .setName("doc_values_facet")
            .setDim("multi_atom_field")
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 26),
        new ExpectedValues(new HashSet<>(Arrays.asList("2", "3")), 25),
        new ExpectedValues(new HashSet<>(Arrays.asList("4", "5", "6")), 24),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 12));
  }

  @Test
  public void testAtomRangeDocValuesFacet() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("atom_field").setTopN(10).build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        9,
        9,
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 2),
        new ExpectedValues(
            new HashSet<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "8")), 1));
  }

  @Test
  public void testAtomRangeMultiDocValuesFacet() {
    Facet facet =
        Facet.newBuilder()
            .setName("doc_values_facet")
            .setDim("multi_atom_field")
            .setTopN(10)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        8,
        8,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1", "2", "3", "4")), 3),
        new ExpectedValues(new HashSet<>(Arrays.asList("5", "6")), 2),
        new ExpectedValues(new HashSet<>(Collections.singletonList("7")), 1));
  }

  @Test
  public void testTopNFacets() {
    Facet facet =
        Facet.newBuilder()
            .setName("doc_values_facet")
            .setDim("multi_atom_field")
            .setTopN(4)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        8,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 26),
        new ExpectedValues(new HashSet<>(Arrays.asList("2", "3")), 25));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testFieldNotIndexable() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("virtual_field").setTopN(1).build();
    doQuery(facet);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testFieldNoDocValues() {
    Facet facet =
        Facet.newBuilder().setName("doc_values_facet").setDim("no_doc_values").setTopN(1).build();
    doQuery(facet);
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
    assertEquals("doc_values_facet", result.getName());
    assertTrue(response.getDiagnostics().containsFacetTimeMs("doc_values_facet"));
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

  private SearchResponse doQuery(Facet facet) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .addFacets(facet)
                .build());
  }

  private SearchResponse doRangeQuery(Facet facet) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("41")
                                .setUpper("50")
                                .build())
                        .build())
                .addFacets(facet)
                .build());
  }
}
