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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class SchemaWarmingQueryGeneratorTest extends ServerTestCase {
  private static final int NUM_DOCS = 50;
  private static final List<String> CATEGORIES = List.of("food", "shopping", "nightlife");

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/warming/registerFieldsSchemaWarming.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields("doc_id", field(String.valueOf(i)))
              .putFields("category", field(CATEGORIES.get(i % CATEGORIES.size())))
              .putFields("description", field("doc number " + i))
              .putFields("count", field(String.valueOf(i)))
              .putFields("price", field(String.valueOf(1000L + i * 10)))
              .putFields("rating", field(String.valueOf(1.0f + (i % 5))))
              .putFields("score", field(String.valueOf(i * 0.5)))
              .putFields("active", field(String.valueOf(i % 2 == 0)))
              .putFields("created", field(String.valueOf(1_700_000_000_000L + i * 1000L)))
              .putFields("updated", field(String.format("2026-01-%02d 10:00:00", 1 + i % 28)))
              .putFields(
                  "embedding", field(String.format("[%d.0, 1.0, %d.0]", 1 + i % 3, 1 + i % 7)))
              .putFields("byte_embedding", field(String.format("[%d, 2, 3, 4]", 1 + i % 50)))
              .build());
    }
    addDocuments(docs.stream());
  }

  private static MultiValuedField field(String value) {
    return MultiValuedField.newBuilder().addValue(value).build();
  }

  private List<SearchRequest> generate(int numQueries) throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    return new SchemaWarmingQueryGenerator(indexState, new Random(42)).generate(numQueries);
  }

  @Test
  public void testGeneratesAllStages() throws IOException {
    List<SearchRequest> requests = generate(100);

    List<SearchRequest> filterRequests = requests.stream().filter(SearchRequest::hasQuery).toList();
    assertEquals(100, filterRequests.size());
    // 60% single field
    assertEquals(
        60,
        filterRequests.stream()
            .filter(r -> r.getQuery().getQueryNodeCase() != Query.QueryNodeCase.BOOLEANQUERY)
            .count());
    assertTrue(
        filterRequests.stream()
            .filter(r -> r.getQuery().hasBooleanQuery())
            .allMatch(r -> r.getQuery().getBooleanQuery().getClausesCount() >= 2));

    // one facet request covering the single faceted field
    List<SearchRequest> facetRequests =
        requests.stream().filter(r -> r.getFacetsCount() > 0).toList();
    assertEquals(1, facetRequests.size());
    assertEquals("category", facetRequests.get(0).getFacets(0).getDim());
    assertEquals(0, facetRequests.get(0).getTopHits());

    // kNN queries for both vector fields
    Set<String> knnFields =
        requests.stream()
            .filter(r -> r.getKnnCount() > 0)
            .map(r -> r.getKnn(0).getField())
            .collect(Collectors.toSet());
    assertEquals(Set.of("embedding", "byte_embedding"), knnFields);
    assertEquals(
        2 * SchemaWarmingQueryGenerator.KNN_QUERIES_PER_FIELD,
        requests.stream().filter(r -> r.getKnnCount() > 0).count());

    // no inference
    assertTrue(requests.stream().allMatch(r -> r.getRescorersCount() == 0));
  }

  @Test
  public void testSingleFieldStageCoversSupportedFields() throws IOException {
    Set<String> fields = new HashSet<>();
    for (SearchRequest request : generate(100)) {
      Query query = request.getQuery();
      if (query.hasTermQuery()) {
        fields.add(query.getTermQuery().getField());
      } else if (query.hasRangeQuery()) {
        fields.add(query.getRangeQuery().getField());
      }
    }
    // TEXT fields are skipped, their analyzed terms do not match how they are queried
    assertEquals(
        Set.of("category", "count", "price", "rating", "score", "active", "created", "updated"),
        fields);
  }

  @Test
  public void testUsesSampledValues() throws IOException {
    for (SearchRequest request : generate(100)) {
      Query query = request.getQuery();
      if (query.hasTermQuery() && query.getTermQuery().getField().equals("category")) {
        assertTrue(CATEGORIES.contains(query.getTermQuery().getTextValue()));
      } else if (query.hasRangeQuery() && query.getRangeQuery().getField().equals("count")) {
        int lower = Integer.parseInt(query.getRangeQuery().getLower());
        int upper = Integer.parseInt(query.getRangeQuery().getUpper());
        assertTrue(lower >= 0 && upper <= NUM_DOCS - 1 && lower <= upper);
      }
    }
  }

  @Test
  public void testRetrievesDocValuesFields() throws IOException {
    SearchRequest first = generate(100).get(0);
    assertEquals(SchemaWarmingQueryGenerator.NUM_RETRIEVE_FIELDS, first.getRetrieveFieldsCount());
  }

  @Test
  public void testAllGeneratedQueriesExecute() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    SearchHandler searchHandler = new SearchHandler(getGlobalState());
    int withHits = 0;
    List<SearchRequest> requests = generate(100);
    for (SearchRequest request : requests) {
      SearchResponse response = searchHandler.handle(indexState, request);
      if (response.getTotalHits().getValue() > 0) {
        withHits++;
      }
    }
    // sampled values match real documents, so most queries return hits
    assertTrue(
        "only " + withHits + " of " + requests.size() + " returned hits",
        withHits > requests.size() / 2);
  }

  @Test
  public void testNoQueriesRequested() throws IOException {
    List<SearchRequest> requests = generate(0);
    assertFalse(requests.stream().anyMatch(SearchRequest::hasQuery));
  }
}
