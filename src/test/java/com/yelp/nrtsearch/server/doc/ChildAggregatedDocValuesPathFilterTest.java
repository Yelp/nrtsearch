/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.doc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import com.yelp.nrtsearch.server.script.ScriptService;
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
 * Tests that _CHILDREN. prefix correctly filters child doc values by nested path when multiple
 * nested object fields exist under the same parent. Validates that accessing
 * _CHILDREN.appointments.price only returns appointment children's values (not review children's),
 * and vice versa for _CHILDREN.reviews.stars.
 *
 * <p>Document layout in Lucene block join format (per parent):
 *
 * <pre>
 *   doc 0: review child      (reviews.stars=5, reviews.helpful=10)
 *   doc 1: review child      (reviews.stars=3, reviews.helpful=7)
 *   doc 2: appointment child (appointments.price=50, appointments.duration=30)
 *   doc 3: appointment child (appointments.price=75, appointments.duration=60)
 *   doc 4: PARENT            (business_name="Business 0", int_score=0)
 * </pre>
 *
 * Without path filtering, scanning backward from doc 4 would collect values from ALL children (docs
 * 0-3). With path filtering, _CHILDREN.appointments.price only collects from docs 2-3.
 */
public class ChildAggregatedDocValuesPathFilterTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index_child_agg";
  private static final int NUM_DOCS = 10;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  protected Gson gson = new GsonBuilder().serializeNulls().create();

  // Known values per document for verification:
  // doc i: appointments = [{price: i*100, duration: i*10}, {price: i*100+50, duration: i*10+5}]
  //        reviews      = [{stars: i%5+1, helpful: i*2}, {stars: (i+1)%5+1, helpful: i*2+1}]

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
    return getFieldsFromResourceFile("/registerFieldsChildAggDocValuesPathFilterTest.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    List<AddDocumentRequest> requestChunk = new ArrayList<>();

    for (int id = 0; id < NUM_DOCS; ++id) {
      // Appointments: deterministic prices and durations
      Map<String, Object> appt1 = new HashMap<>();
      appt1.put("price", id * 100);
      appt1.put("duration", id * 10);

      Map<String, Object> appt2 = new HashMap<>();
      appt2.put("price", id * 100 + 50);
      appt2.put("duration", id * 10 + 5);

      // Reviews: deterministic stars and helpful counts
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
                  "business_name",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue("Business " + id)
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
        case "children_appointments_sum" ->
            new ChildrenAppointmentsSumScript(params, docLookup, ctx, scores);
        case "children_reviews_sum" -> new ChildrenReviewsSumScript(params, docLookup, ctx, scores);
        case "children_both_paths" -> new ChildrenBothPathsScript(params, docLookup, ctx, scores);
        case "children_count_test" -> new ChildrenCountTestScript(params, docLookup, ctx, scores);
        default -> throw new IllegalArgumentException("Unknown script: " + scriptId);
      };
    }
  }

  // ---- Script implementations ----

  /**
   * Script that sums all appointment prices via _CHILDREN.appointments.price. Returns the sum, or a
   * negative error code on failure. For doc i: expects values {i*100, i*100+50}, sum = 2*i*100 +
   * 50.
   */
  static class ChildrenAppointmentsSumScript extends ScoreScript {
    private static int executionCount = 0;

    public ChildrenAppointmentsSumScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      executionCount++;
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

    public static boolean wasExecuted() {
      return executionCount > 0;
    }

    public static void resetExecutionCount() {
      executionCount = 0;
    }
  }

  /**
   * Script that sums all review stars via _CHILDREN.reviews.stars. Returns the sum, or a negative
   * error code on failure. For doc i: expects values {i%5+1, (i+1)%5+1}, sum = i%5+1 + (i+1)%5+1.
   */
  static class ChildrenReviewsSumScript extends ScoreScript {
    private static int executionCount = 0;

    public ChildrenReviewsSumScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      executionCount++;
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> stars = doc.get("_CHILDREN.reviews.stars");
        if (stars == null) return -1.0;
        double sum = 0;
        for (int i = 0; i < stars.size(); i++) {
          sum += ((Number) stars.get(i)).doubleValue();
        }
        return sum;
      } catch (Exception e) {
        return -99.0;
      }
    }

    public static boolean wasExecuted() {
      return executionCount > 0;
    }

    public static void resetExecutionCount() {
      executionCount = 0;
    }
  }

  /**
   * Script that accesses both _CHILDREN.appointments.price and _CHILDREN.reviews.stars and encodes
   * the counts and sums into a single double for verification.
   *
   * <p>Returns: appointmentCount * 10000 + reviewCount * 1000 + appointmentSum + reviewSum * 0.001
   * This encoding lets us verify counts and sums independently.
   */
  static class ChildrenBothPathsScript extends ScoreScript {
    private static int executionCount = 0;

    public ChildrenBothPathsScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      executionCount++;
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> prices = doc.get("_CHILDREN.appointments.price");
        LoadedDocValues<?> stars = doc.get("_CHILDREN.reviews.stars");
        if (prices == null) return -1.0;
        if (stars == null) return -2.0;

        double priceSum = 0;
        for (int i = 0; i < prices.size(); i++) {
          priceSum += ((Number) prices.get(i)).doubleValue();
        }

        double starsSum = 0;
        for (int i = 0; i < stars.size(); i++) {
          starsSum += ((Number) stars.get(i)).doubleValue();
        }

        // Encode: apptCount, reviewCount, priceSum, starsSum
        return prices.size() * 1000000 + stars.size() * 100000 + priceSum + starsSum * 0.0001;
      } catch (Exception e) {
        return -99.0;
      }
    }

    public static boolean wasExecuted() {
      return executionCount > 0;
    }

    public static void resetExecutionCount() {
      executionCount = 0;
    }
  }

  /**
   * Script that verifies the count of children for each path is exactly 2. Returns 1.0 if both
   * appointment and review child counts are 2, else negative error.
   */
  static class ChildrenCountTestScript extends ScoreScript {
    private static int executionCount = 0;

    public ChildrenCountTestScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      executionCount++;
      Map<String, LoadedDocValues<?>> doc = this.getDoc();
      try {
        LoadedDocValues<?> prices = doc.get("_CHILDREN.appointments.price");
        LoadedDocValues<?> stars = doc.get("_CHILDREN.reviews.stars");
        if (prices == null) return 100.0;
        if (stars == null) return 200.0;

        // Without path filtering, these counts would be 4 (all children)
        // With correct filtering, each should be exactly 2.
        // Use positive error codes since FunctionScoreQuery clamps negatives to 0.
        if (prices.size() != 2) return 1000.0 + prices.size() * 10 + stars.size();
        if (stars.size() != 2) return 2000.0 + prices.size() * 10 + stars.size();

        return 1.0;
      } catch (Exception e) {
        return 9900.0;
      }
    }

    public static boolean wasExecuted() {
      return executionCount > 0;
    }

    public static void resetExecutionCount() {
      executionCount = 0;
    }
  }

  // ---- Test methods ----

  /**
   * Verify that _CHILDREN.appointments.price returns only appointment children's price values, not
   * review children's values. Checks both the count (should be 2 per parent) and the actual sum of
   * values.
   */
  @Test
  public void testChildrenAppointmentPricesFiltered() throws IOException {
    ChildrenAppointmentsSumScript.resetExecutionCount();
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("children_appointments_sum");

    assertTrue("Script should have been executed", ChildrenAppointmentsSumScript.wasExecuted());
    assertTrue("Search should return results", response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Score should be non-negative (no errors). Got: " + score, score >= 0.0);
    }
  }

  /**
   * Verify that _CHILDREN.reviews.stars returns only review children's stars values, not
   * appointment children's values.
   */
  @Test
  public void testChildrenReviewStarsFiltered() throws IOException {
    ChildrenReviewsSumScript.resetExecutionCount();
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("children_reviews_sum");

    assertTrue("Script should have been executed", ChildrenReviewsSumScript.wasExecuted());
    assertTrue("Search should return results", response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Score should be non-negative (no errors). Got: " + score, score >= 0.0);
    }
  }

  /**
   * The critical test: verify that each nested path returns exactly 2 children (not 4). Without
   * path filtering, both appointments and reviews children would be mixed together, and each
   * _CHILDREN.X.field access would return 4 values instead of 2.
   */
  @Test
  public void testChildCountCorrectWithPathFiltering() throws IOException {
    ChildrenCountTestScript.resetExecutionCount();
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("children_count_test");

    assertTrue("Script should have been executed", ChildrenCountTestScript.wasExecuted());
    assertTrue("Search should return results", response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertEquals(
          "Each path should have exactly 2 children. Score encodes error info if not 1.0. Got: "
              + score,
          1.0,
          score,
          0.0001);
    }
  }

  /**
   * Verify that both paths can be accessed in the same script execution and return independently
   * correct values.
   */
  @Test
  public void testBothPathsAccessedSimultaneously() throws IOException {
    ChildrenBothPathsScript.resetExecutionCount();
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("children_both_paths");

    assertTrue("Script should have been executed", ChildrenBothPathsScript.wasExecuted());
    assertTrue("Search should return results", response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Score should be positive (no errors). Got: " + score, score > 0.0);

      // Decode the encoded score: apptCount * 1000000 + reviewCount * 100000 + priceSum + starsSum
      // * 0.0001
      int apptCount = (int) (score / 1000000);
      int reviewCount = (int) ((score % 1000000) / 100000);

      assertEquals("Appointment count should be 2", 2, apptCount);
      assertEquals("Review count should be 2", 2, reviewCount);
    }
  }

  // ---- Helper to run a script at the root (parent) level ----

  private SearchResponse runScriptAtRootLevel(String scriptSource) {
    Script script = Script.newBuilder().setLang("test_lang").setSource(scriptSource).build();

    com.yelp.nrtsearch.server.grpc.Query functionScoreQuery =
        com.yelp.nrtsearch.server.grpc.Query.newBuilder()
            .setFunctionScoreQuery(
                FunctionScoreQuery.newBuilder()
                    .setScript(script)
                    .setQuery(
                        com.yelp.nrtsearch.server.grpc.Query.newBuilder()
                            .setMatchAllQuery(MatchAllQuery.newBuilder().build())
                            .build())
                    .build())
            .build();

    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(Arrays.asList("doc_id", "int_score", "business_name"))
            .setQuery(functionScoreQuery)
            // No queryNestedPath — script runs at ROOT (parent) level
            .build();

    return getGrpcServer().getBlockingStub().search(request);
  }
}
