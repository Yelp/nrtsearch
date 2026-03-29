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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import com.yelp.nrtsearch.server.script.ScriptService;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DoubleValues;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for query-level child filtering via the {@code childFilters} field on {@link
 * SearchRequest}. Validates that user-provided filters are ANDed with the automatic nested path
 * filter, narrowing which child documents are included in {@code _CHILDREN.} aggregation.
 *
 * <p>Uses the same schema as ChildAggregatedDocValuesPathFilterTest: parent documents with two
 * nested object types (appointments and reviews). Appointments have a price field that allows
 * range-based filtering.
 *
 * <p>Document layout per parent (doc i):
 *
 * <pre>
 *   appointments: [{price: i*100, duration: i*10}, {price: i*100+50, duration: i*10+5}]
 *   reviews:      [{stars: i%5+1, helpful: i*2}, {stars: (i+1)%5+1, helpful: i*2+1}]
 * </pre>
 */
public class ChildFilterIntegrationTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index_child_filter";
  private static final int NUM_DOCS = 10;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  protected Gson gson = new GsonBuilder().serializeNulls().create();

  private void init(List<Plugin> plugins) {
    ScriptService.initialize(getEmptyConfig(), plugins);
  }

  private NrtsearchConfig getEmptyConfig() {
    String config = "nodeName: \"server_foo\"";
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsChildFilterTest.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    List<AddDocumentRequest> requestChunk = new ArrayList<>();

    for (int id = 0; id < NUM_DOCS; ++id) {
      // Appointments: deterministic prices
      // appt1: price = id * 100       (e.g., doc 5: price=500)
      // appt2: price = id * 100 + 50  (e.g., doc 5: price=550)
      Map<String, Object> appt1 = new HashMap<>();
      appt1.put("price", id * 100);
      appt1.put("duration", id * 10);

      Map<String, Object> appt2 = new HashMap<>();
      appt2.put("price", id * 100 + 50);
      appt2.put("duration", id * 10 + 5);

      // Reviews: deterministic stars
      Map<String, Object> review1 = new HashMap<>();
      review1.put("stars", id % 5 + 1);
      review1.put("helpful", id * 2);

      Map<String, Object> review2 = new HashMap<>();
      review2.put("stars", (id + 1) % 5 + 1);
      review2.put("helpful", id * 2 + 1);

      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "appointments",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(gson.toJson(appt1))
                      .addValue(gson.toJson(appt2))
                      .build())
              .putFields(
                  "reviews",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(gson.toJson(review1))
                      .addValue(gson.toJson(review2))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }

    if (!requestChunk.isEmpty()) {
      addDocuments(requestChunk.stream());
      writer.commit();
    }
  }

  // ---- Script plugin infrastructure ----

  static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestScriptEngine());
    }
  }

  public static class TestScriptEngine implements ScriptEngine {
    @Override
    public String getLang() {
      return "test_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      ScoreScript.Factory factory =
          ((params, docLookup) -> new TestScriptFactory(params, docLookup, source));
      return context.factoryClazz.cast(factory);
    }
  }

  static class TestScriptFactory extends ScoreScript.SegmentFactory {
    private final Map<String, Object> params;
    private final DocLookup docLookup;
    private final String scriptId;

    public TestScriptFactory(Map<String, Object> params, DocLookup docLookup, String scriptId) {
      super(params, docLookup);
      this.params = params;
      this.docLookup = docLookup;
      this.scriptId = scriptId;
    }

    @Override
    public boolean needs_score() {
      return true;
    }

    @Override
    public DoubleValues newInstance(LeafReaderContext ctx, DoubleValues scores) {
      return switch (scriptId) {
        case "count_appointments" -> new CountAppointmentsScript(params, docLookup, ctx, scores);
        case "sum_appointment_prices" ->
            new SumAppointmentPricesScript(params, docLookup, ctx, scores);
        case "count_reviews" -> new CountReviewsScript(params, docLookup, ctx, scores);
        default -> throw new IllegalArgumentException("Unknown script: " + scriptId);
      };
    }
  }

  // ---- Script implementations ----

  /** Returns the count of appointment children visible via _CHILDREN.appointments.price. */
  static class CountAppointmentsScript extends ScoreScript {
    public CountAppointmentsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> prices = doc.get("_CHILDREN.appointments.price");
        if (prices == null) return -1.0;
        return prices.size();
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  /** Returns the sum of appointment prices visible via _CHILDREN.appointments.price. */
  static class SumAppointmentPricesScript extends ScoreScript {
    public SumAppointmentPricesScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> prices = doc.get("_CHILDREN.appointments.price");
        if (prices == null) return -1.0;
        double sum = 0;
        for (int i = 0; i < prices.size(); i++) {
          sum += ((Number) prices.get(i)).doubleValue();
        }
        return sum;
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  /** Returns the count of review children visible via _CHILDREN.reviews.stars. */
  static class CountReviewsScript extends ScoreScript {
    public CountReviewsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> stars = doc.get("_CHILDREN.reviews.stars");
        if (stars == null) return -1.0;
        return stars.size();
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  // ---- Helper methods ----

  private SearchResponse runScriptWithChildFilters(
      String scriptSource, List<ChildFilter> childFilters) {
    init(Collections.singletonList(new TestScriptPlugin()));

    Script script = Script.newBuilder().setLang("test_lang").setSource(scriptSource).build();

    Query functionScoreQuery =
        Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(script)
                    .setQuery(
                        Query.newBuilder()
                            .setMatchAllQuery(MatchAllQuery.newBuilder().build())
                            .build())
                    .build())
            .build();

    SearchRequest.Builder requestBuilder =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(Arrays.asList("doc_id", "int_score"))
            .setQuery(functionScoreQuery);

    for (ChildFilter cf : childFilters) {
      requestBuilder.addChildFilters(cf);
    }

    return getGrpcServer().getBlockingStub().search(requestBuilder.build());
  }

  // ---- Tests ----

  /**
   * Without any child filter, each parent should see 2 appointments. This is the baseline to
   * confirm the test setup is correct.
   */
  @Test
  public void testNoFilterReturnsAllChildren() {
    SearchResponse response = runScriptWithChildFilters("count_appointments", List.of());

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "Without filter, each parent should have 2 appointment children",
          2.0,
          hit.getScore(),
          0.0001);
    }
  }

  /**
   * Apply a range filter on appointments.price that excludes the lower-priced appointment. For doc
   * i, prices are {i*100, i*100+50}. Filter: price >= i*100+50 would leave only 1 appointment per
   * parent. We use a fixed threshold of 50 so that for doc 0 (prices 0, 50) only the second matches
   * and for doc 1+ (prices >= 100) the first also matches.
   *
   * <p>With filter price >= 50: doc 0 has prices {0, 50} -> only {50} matches (count=1). doc 1+ has
   * prices {100+, 150+} -> both match (count=2).
   */
  @Test
  public void testRangeFilterNarrowsAppointments() {
    ChildFilter priceFilter =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("appointments.price")
                            .setLower("50")
                            .setUpper(String.valueOf(Integer.MAX_VALUE))
                            .build())
                    .build())
            .build();

    SearchResponse response = runScriptWithChildFilters("count_appointments", List.of(priceFilter));

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double count = hit.getScore();
      int docId =
          hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue().isEmpty()
              ? 0
              : Integer.parseInt(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());

      if (docId == 0) {
        // doc 0: prices are {0, 50}. Filter >= 50 matches only {50}
        assertEquals("Doc 0 should have 1 appointment after filter", 1.0, count, 0.0001);
      } else {
        // doc 1+: both prices are >= 50
        assertEquals(
            "Doc " + docId + " should have 2 appointments after filter", 2.0, count, 0.0001);
      }
    }
  }

  /**
   * Apply a filter that matches NO appointments. All children should be excluded, so the script
   * returns count=0 for every parent.
   */
  @Test
  public void testFilterMatchingNoneReturnsEmpty() {
    ChildFilter noMatchFilter =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("appointments.price")
                            .setLower(String.valueOf(Integer.MAX_VALUE - 1))
                            .setUpper(String.valueOf(Integer.MAX_VALUE))
                            .build())
                    .build())
            .build();

    SearchResponse response =
        runScriptWithChildFilters("count_appointments", List.of(noMatchFilter));

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "No appointments should match the impossible filter", 0.0, hit.getScore(), 0.0001);
    }
  }

  /**
   * Apply a child filter only on appointments. Reviews should be unaffected — each parent should
   * still see 2 reviews.
   */
  @Test
  public void testFilterOnOnePathDoesNotAffectOtherPath() {
    // Filter appointments to match none
    ChildFilter noMatchApptFilter =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("appointments.price")
                            .setLower(String.valueOf(Integer.MAX_VALUE - 1))
                            .setUpper(String.valueOf(Integer.MAX_VALUE))
                            .build())
                    .build())
            .build();

    // Run the reviews count script with the appointments filter
    SearchResponse response =
        runScriptWithChildFilters("count_reviews", List.of(noMatchApptFilter));

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "Reviews should be unaffected by appointments filter", 2.0, hit.getScore(), 0.0001);
    }
  }

  /**
   * Verify that the sum of filtered appointment prices is correct. For doc i with filter price >=
   * 50: doc 0 -> only price 50 -> sum=50. doc 1+ -> prices {i*100, i*100+50} both match -> sum =
   * 2*i*100 + 50.
   */
  @Test
  public void testFilteredSumIsCorrect() {
    ChildFilter priceFilter =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("appointments.price")
                            .setLower("50")
                            .setUpper(String.valueOf(Integer.MAX_VALUE))
                            .build())
                    .build())
            .build();

    SearchResponse response =
        runScriptWithChildFilters("sum_appointment_prices", List.of(priceFilter));

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double sum = hit.getScore();
      int docId = Integer.parseInt(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());

      if (docId == 0) {
        assertEquals("Doc 0 filtered sum should be 50", 50.0, sum, 0.0001);
      } else {
        double expectedSum = 2.0 * docId * 100 + 50;
        assertEquals("Doc " + docId + " filtered sum", expectedSum, sum, 0.0001);
      }
    }
  }

  /** Verify that specifying a non-nested path in ChildFilter throws an error. */
  @Test(expected = StatusRuntimeException.class)
  public void testInvalidPathThrowsError() {
    ChildFilter invalidFilter =
        ChildFilter.newBuilder()
            .setPath("int_score")
            .setFilter(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("int_score")
                            .setLower("0")
                            .setUpper("100")
                            .build())
                    .build())
            .build();

    runScriptWithChildFilters("count_appointments", List.of(invalidFilter));
  }

  /** Verify that duplicate paths in ChildFilter throws an error. */
  @Test(expected = StatusRuntimeException.class)
  public void testDuplicatePathThrowsError() {
    ChildFilter filter1 =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()).build())
            .build();
    ChildFilter filter2 =
        ChildFilter.newBuilder()
            .setPath("appointments")
            .setFilter(
                Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()).build())
            .build();

    runScriptWithChildFilters("count_appointments", List.of(filter1, filter2));
  }
}
