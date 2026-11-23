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

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.field.LatLonFieldDef;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

/** Structured schema warming queries over nested, geo, polygon and text fields. */
public class SchemaWarmingStructuredQueriesTest extends ServerTestCase {
  private static final Gson GSON = new Gson();
  private static final int NUM_DOCS = 60;
  private static final List<String> WORDS =
      List.of("pizza", "sushi", "tacos", "burgers", "coffee", "bakery", "noodles", "salad");
  private static final double BASE_LAT = 37.7;
  private static final double BASE_LON = -122.4;

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/warming/registerFieldsSchemaWarmingStructured.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      String word = WORDS.get(i % WORDS.size());
      String other = WORDS.get((i * 3 + 1) % WORDS.size());
      double lat = BASE_LAT + i * 0.001;
      double lon = BASE_LON + i * 0.001;
      // a small square around the point
      List<List<Double>> ring =
          List.of(
              List.of(lon - 0.01, lat - 0.01),
              List.of(lon + 0.01, lat - 0.01),
              List.of(lon + 0.01, lat + 0.01),
              List.of(lon - 0.01, lat + 0.01),
              List.of(lon - 0.01, lat - 0.01));
      Map<String, Object> polygon = Map.of("type", "Polygon", "coordinates", List.of(ring));
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields("doc_id", field(String.valueOf(i)))
              .putFields("category", field("cat" + (i % 4)))
              .putFields("count", field(String.valueOf(i)))
              .putFields("title", field(word + " place " + other))
              .putFields("body", field("the best " + other + " and " + word + " in town"))
              .putFields(
                  "location",
                  MultiValuedField.newBuilder()
                      .addValue(String.valueOf(lat))
                      .addValue(String.valueOf(lon))
                      .build())
              .putFields("area", field(GSON.toJson(polygon)))
              .putFields(
                  "bids",
                  MultiValuedField.newBuilder()
                      .addValue(GSON.toJson(Map.of("amount", i, "campaign", "c" + (i % 3))))
                      .addValue(GSON.toJson(Map.of("amount", i + 100, "campaign", "c" + (i % 5))))
                      .build())
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

  /** Every query node of the request, walking boolean clauses and nested queries. */
  static List<Query> allQueries(SearchRequest request) {
    List<Query> queries = new ArrayList<>();
    if (request.hasQuery()) {
      collect(request.getQuery(), queries);
    }
    return queries;
  }

  private static void collect(Query query, List<Query> queries) {
    queries.add(query);
    if (query.hasBooleanQuery()) {
      for (BooleanClause clause : query.getBooleanQuery().getClausesList()) {
        collect(clause.getQuery(), queries);
      }
    } else if (query.hasNestedQuery()) {
      collect(query.getNestedQuery().getQuery(), queries);
    }
  }

  private static Set<Query.QueryNodeCase> queryTypes(List<SearchRequest> requests) {
    return requests.stream()
        .flatMap(r -> allQueries(r).stream())
        .map(Query::getQueryNodeCase)
        .collect(Collectors.toSet());
  }

  @Test
  public void testGeneratesStructuredQueryTypes() throws IOException {
    List<SearchRequest> requests = generate(400);
    Set<Query.QueryNodeCase> types = queryTypes(requests);
    for (Query.QueryNodeCase type :
        List.of(
            Query.QueryNodeCase.NESTEDQUERY,
            Query.QueryNodeCase.GEORADIUSQUERY,
            Query.QueryNodeCase.GEOBOUNDINGBOXQUERY,
            Query.QueryNodeCase.GEOPOINTQUERY,
            Query.QueryNodeCase.EXISTSQUERY,
            Query.QueryNodeCase.MATCHQUERY,
            Query.QueryNodeCase.MATCHPHRASEQUERY,
            Query.QueryNodeCase.MULTIMATCHQUERY)) {
      assertTrue("no " + type + " query generated", types.contains(type));
    }
    // distance sort from a sampled point
    assertTrue(
        requests.stream()
            .filter(SearchRequest::hasQuerySort)
            .flatMap(r -> r.getQuerySort().getFields().getSortedFieldsList().stream())
            .anyMatch(s -> s.getFieldName().equals("location") && s.hasOrigin()));
    // field sort
    assertTrue(
        requests.stream()
            .filter(SearchRequest::hasQuerySort)
            .flatMap(r -> r.getQuerySort().getFields().getSortedFieldsList().stream())
            .map(SortType::getFieldName)
            .anyMatch(f -> f.equals("count") || f.equals("category")));
    // js scripts, the painless plugin is not installed in tests
    assertTrue(
        requests.stream()
            .flatMap(r -> r.getVirtualFieldsList().stream())
            .allMatch(vf -> vf.getScript().getLang().equals(SchemaWarmingQueryGenerator.JS_LANG)));
    assertTrue(requests.stream().anyMatch(r -> r.getVirtualFieldsCount() > 0));
    assertTrue(requests.stream().anyMatch(r -> r.getCollectorsCount() > 0));
    // several clauses of one nested document in one nested query
    assertTrue(
        requests.stream()
            .flatMap(r -> allQueries(r).stream())
            .filter(Query::hasNestedQuery)
            .anyMatch(
                q ->
                    q.getNestedQuery().getQuery().hasBooleanQuery()
                        && q.getNestedQuery().getQuery().getBooleanQuery().getClausesCount() >= 2));
    // minimum should match on boolean queries
    assertTrue(
        requests.stream()
            .flatMap(r -> allQueries(r).stream())
            .anyMatch(
                q -> q.hasBooleanQuery() && q.getBooleanQuery().getMinimumNumberShouldMatch() > 0));
  }

  @Test
  public void testNestedFieldsAreQueriedThroughNestedQuery() throws IOException {
    for (SearchRequest request : generate(400)) {
      checkNested(request.getQuery(), false);
    }
  }

  private static void checkNested(Query query, boolean insideNested) {
    String field =
        switch (query.getQueryNodeCase()) {
          case TERMQUERY -> query.getTermQuery().getField();
          case TERMINSETQUERY -> query.getTermInSetQuery().getField();
          case RANGEQUERY -> query.getRangeQuery().getField();
          default -> null;
        };
    if (field != null && field.startsWith("bids.")) {
      assertTrue("nested field " + field + " queried from the root document", insideNested);
    }
    if (query.hasBooleanQuery()) {
      query
          .getBooleanQuery()
          .getClausesList()
          .forEach(c -> checkNested(c.getQuery(), insideNested));
    } else if (query.hasNestedQuery()) {
      assertEquals("bids", query.getNestedQuery().getPath());
      checkNested(query.getNestedQuery().getQuery(), true);
    }
  }

  @Test
  public void testChildFieldsAreQueried() throws IOException {
    // the TEXT child field is used in match queries, the root of the nested path is not a field
    Set<String> matchFields =
        generate(400).stream()
            .flatMap(r -> allQueries(r).stream())
            .filter(Query::hasMatchQuery)
            .map(q -> q.getMatchQuery().getField())
            .collect(Collectors.toSet());
    assertTrue(matchFields.toString(), matchFields.contains("category.text"));
  }

  @Test
  public void testAllGeneratedQueriesExecute() throws Exception {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    SearchHandler searchHandler = new SearchHandler(getGlobalState());
    List<SearchRequest> requests = generate(400);
    int withHits = 0;
    int nestedWithHits = 0;
    int geoWithHits = 0;
    for (SearchRequest request : requests) {
      SearchResponse response = searchHandler.handle(indexState, request);
      if (response.getTotalHits().getValue() == 0) {
        continue;
      }
      withHits++;
      Set<Query.QueryNodeCase> types = queryTypes(List.of(request));
      if (types.contains(Query.QueryNodeCase.NESTEDQUERY)) {
        nestedWithHits++;
      }
      if (types.contains(Query.QueryNodeCase.GEORADIUSQUERY)
          || types.contains(Query.QueryNodeCase.GEOPOINTQUERY)) {
        geoWithHits++;
      }
    }
    assertTrue(
        "only " + withHits + " of " + requests.size() + " returned hits",
        withHits > requests.size() / 2);
    assertTrue(nestedWithHits > 0);
    assertTrue(geoWithHits > 0);
  }

  @Test
  public void testSampleGeoPoints() throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    var shardState = indexState.getShard(0);
    var searcherAndTaxonomy = shardState.acquire();
    try {
      List<double[]> points =
          SchemaWarmingQueryGenerator.sampleGeoPoints(
              searcherAndTaxonomy.searcher(),
              (LatLonFieldDef) indexState.getFieldOrThrow("location"),
              new Random(7));
      assertFalse(points.isEmpty());
      for (double[] point : points) {
        assertTrue(point[0] >= BASE_LAT - 1e-6 && point[0] <= BASE_LAT + NUM_DOCS * 0.001);
        assertTrue(point[1] >= BASE_LON - 1e-6 && point[1] <= BASE_LON + NUM_DOCS * 0.001);
      }
    } finally {
      shardState.release(searcherAndTaxonomy);
    }
  }

  @Test
  public void testSampleTextTerms() throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    var shardState = indexState.getShard(0);
    var searcherAndTaxonomy = shardState.acquire();
    try {
      List<String> terms =
          SchemaWarmingQueryGenerator.sampleTextTerms(
              searcherAndTaxonomy.searcher(),
              (com.yelp.nrtsearch.server.field.IndexableFieldDef<?>)
                  indexState.getFieldOrThrow("title"),
              new Random(7));
      assertFalse(terms.isEmpty());
      Set<String> vocabulary = new java.util.HashSet<>(WORDS);
      vocabulary.add("place");
      assertTrue(terms.toString(), vocabulary.containsAll(terms));
    } finally {
      shardState.release(searcherAndTaxonomy);
    }
  }

  @Test
  public void testMaxTopHitsCapsQueries() throws IOException {
    IndexState indexState = getGlobalState().getIndexOrThrow(DEFAULT_TEST_INDEX);
    List<SearchRequest> requests =
        new SchemaWarmingQueryGenerator(indexState, new Random(42), 100).generate(200);
    assertTrue(requests.stream().allMatch(r -> r.getTopHits() <= 100));
    assertFalse(requests.isEmpty());
  }
}
