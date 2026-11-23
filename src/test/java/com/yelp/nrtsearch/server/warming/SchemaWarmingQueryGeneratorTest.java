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
import com.yelp.nrtsearch.server.grpc.BooleanClause;
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
  private static final List<String> LABELS = List.of("new", "popular");

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
              .putFields("label", field(LABELS.get(i % LABELS.size())))
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

  /** Requests of the filter stage: term, term set and range leaves under FILTER clauses. */
  static boolean isFilterRequest(SearchRequest request) {
    if (!request.hasQuery()
        || request.hasQuerySort()
        || request.getVirtualFieldsCount() > 0
        || request.getCollectorsCount() > 0) {
      return false;
    }
    Query query = request.getQuery();
    if (query.hasBooleanQuery()) {
      return query.getBooleanQuery().getClausesList().stream()
          .allMatch(c -> c.getOccur() == BooleanClause.Occur.FILTER && isLeaf(c.getQuery()));
    }
    return isLeaf(query);
  }

  private static boolean isLeaf(Query query) {
    return query.hasTermQuery() || query.hasTermInSetQuery() || query.hasRangeQuery();
  }

  private List<SearchRequest> filterRequests(int numQueries) throws IOException {
    return generate(numQueries).stream()
        .filter(SchemaWarmingQueryGeneratorTest::isFilterRequest)
        .toList();
  }

  @Test
  public void testGeneratesAllStages() throws IOException {
    List<SearchRequest> requests = generate(100);

    // half filter queries, the other half structured
    List<SearchRequest> filterRequests =
        requests.stream().filter(SchemaWarmingQueryGeneratorTest::isFilterRequest).toList();
    assertEquals(50, filterRequests.size());
    // 60% of the filter queries are single field, term set queries are single field too
    assertEquals(
        30,
        filterRequests.stream()
            .filter(r -> r.getQuery().getQueryNodeCase() != Query.QueryNodeCase.BOOLEANQUERY)
            .count());
    assertTrue(
        filterRequests.stream()
            .filter(r -> r.getQuery().hasBooleanQuery())
            .allMatch(r -> r.getQuery().getBooleanQuery().getClausesCount() >= 2));

    // structured queries for the kinds this index supports, it has no geo or nested fields
    assertTrue(requests.stream().anyMatch(SearchRequest::hasQuerySort));
    assertTrue(requests.stream().anyMatch(r -> r.getVirtualFieldsCount() > 0));
    assertTrue(requests.stream().anyMatch(r -> r.getCollectorsCount() > 0));
    assertTrue(
        requests.stream()
            .flatMap(r -> SchemaWarmingStructuredQueriesTest.allQueries(r).stream())
            .anyMatch(Query::hasMatchQuery));
    assertTrue(
        requests.stream()
            .filter(r -> r.getQuery().hasBooleanQuery())
            .flatMap(r -> r.getQuery().getBooleanQuery().getClausesList().stream())
            .anyMatch(c -> c.getOccur() == BooleanClause.Occur.MUST_NOT));

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
    for (SearchRequest request : filterRequests(100)) {
      Query query = request.getQuery();
      if (query.hasTermQuery()) {
        fields.add(query.getTermQuery().getField());
      } else if (query.hasTermInSetQuery()) {
        fields.add(query.getTermInSetQuery().getField());
      } else if (query.hasRangeQuery()) {
        fields.add(query.getRangeQuery().getField());
      }
    }
    // TEXT fields are only used in match queries, their analyzed terms do not match term queries
    assertEquals(
        Set.of(
            "category",
            "label",
            "count",
            "price",
            "rating",
            "score",
            "active",
            "created",
            "updated"),
        fields);
  }

  @Test
  public void testUsesSampledValues() throws IOException {
    for (SearchRequest request : generate(100)) {
      Query query = request.getQuery();
      if (query.hasTermQuery() && query.getTermQuery().getField().equals("category")) {
        assertTrue(CATEGORIES.contains(query.getTermQuery().getTextValue()));
      } else if (query.hasTermInSetQuery()
          && query.getTermInSetQuery().getField().equals("category")) {
        assertTrue(CATEGORIES.containsAll(query.getTermInSetQuery().getTextTerms().getTermsList()));
      } else if (query.hasRangeQuery() && query.getRangeQuery().getField().equals("count")) {
        int lower = Integer.parseInt(query.getRangeQuery().getLower());
        int upper = Integer.parseInt(query.getRangeQuery().getUpper());
        assertTrue(lower >= 0 && upper <= NUM_DOCS - 1 && lower <= upper);
      }
    }
  }

  @Test
  public void testRetrievesDocValuesFields() throws IOException {
    List<SearchRequest> filterRequests =
        generate(100).stream().filter(SearchRequest::hasQuery).toList();
    assertTrue(filterRequests.stream().allMatch(r -> r.getRetrieveFieldsCount() >= 1));
    // some requests retrieve many fields, so wide fetches are warmed too
    assertTrue(filterRequests.stream().anyMatch(r -> r.getRetrieveFieldsCount() >= 5));
    Set<String> retrieved =
        filterRequests.stream()
            .flatMap(r -> r.getRetrieveFieldsList().stream())
            .collect(Collectors.toSet());
    assertTrue(retrieved.containsAll(Set.of("category", "count", "price", "score", "active")));
  }

  @Test
  public void testVariesTopHits() throws IOException {
    List<SearchRequest> requests = generate(100);
    Set<Integer> topHits =
        requests.stream()
            .filter(SchemaWarmingQueryGeneratorTest::isFilterRequest)
            .map(SearchRequest::getTopHits)
            .collect(Collectors.toSet());
    assertEquals(Set.of(10, 30, 100), topHits);
    // structured queries fetch up to a few hundred hits, like queries that are rescored
    assertTrue(requests.stream().anyMatch(r -> r.getTopHits() > 100));
  }

  @Test
  public void testGeneratesTermSetQueries() throws IOException {
    List<Query> termSets =
        generate(100).stream()
            .map(SearchRequest::getQuery)
            .filter(Query::hasTermInSetQuery)
            .toList();
    assertFalse(termSets.isEmpty());
    for (Query query : termSets) {
      int size = query.getTermInSetQuery().getTextTerms().getTermsCount();
      assertTrue(size >= 2 && size <= SchemaWarmingQueryGenerator.MAX_TERM_SET_SIZE);
    }
  }

  @Test
  public void testNoDuplicateQueries() throws IOException {
    List<SearchRequest> requests = generate(100);
    List<Query> queries =
        requests.stream()
            .filter(SchemaWarmingQueryGeneratorTest::isFilterRequest)
            .map(SearchRequest::getQuery)
            .toList();
    assertEquals(queries.size(), new HashSet<>(queries).size());
    // structured requests are distinct too, kNN requests may repeat a seed vector
    List<SearchRequest> withQuery = requests.stream().filter(SearchRequest::hasQuery).toList();
    assertEquals(withQuery.size(), new HashSet<>(withQuery).size());
  }

  @Test
  public void testSampleDocValuesReadsLiveValues() throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    var shardState = indexState.getShard(0);
    var searcherAndTaxonomy = shardState.acquire();
    try {
      List<String> values =
          SchemaWarmingQueryGenerator.sampleDocValues(
              searcherAndTaxonomy.searcher(), "category", new Random(7));
      // all three categories are common, so random documents find each of them
      assertEquals(Set.copyOf(CATEGORIES), Set.copyOf(values));
      // single valued text fields with binary doc values are sampled too
      assertEquals(
          Set.copyOf(LABELS),
          Set.copyOf(
              SchemaWarmingQueryGenerator.sampleDocValues(
                  searcherAndTaxonomy.searcher(), "label", new Random(7))));
    } finally {
      shardState.release(searcherAndTaxonomy);
    }
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
  public void testMaxTopHitsCapsQueries() throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    List<SearchRequest> requests =
        new SchemaWarmingQueryGenerator(indexState, new Random(42), 50).generate(100);
    assertTrue(requests.stream().allMatch(r -> r.getTopHits() <= 50));
  }

  @Test
  public void testNoQueriesRequested() throws IOException {
    List<SearchRequest> requests = generate(0);
    assertFalse(requests.stream().anyMatch(SearchRequest::hasQuery));
  }
}
