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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import com.yelp.nrtsearch.server.script.ScriptFactoryContext;
import com.yelp.nrtsearch.server.script.ScriptService;
import io.grpc.testing.GrpcCleanupRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests that verify _PARENT. and _CHILDREN. script-level doc-value access works correctly for
 * multi-level nested documents (orders → items).
 *
 * <p>Uses the same schema as MultiLevelNestedTest so no separate resource file is needed. The index
 * is independent of MultiLevelNestedTest's index to avoid cross-test state contamination.
 */
public class MultiLevelNestedScriptAccessTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  @Override
  public List<String> getIndices() {
    return List.of(DEFAULT_TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsMultiLevelNested.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    addDocuments(buildTestDocs(name).stream());
  }

  private List<AddDocumentRequest> buildTestDocs(String indexName) {
    List<AddDocumentRequest> docs = new ArrayList<>();

    Map<String, Object> item1 = new LinkedHashMap<>();
    item1.put("item_name", "widget");
    item1.put("quantity", 2);

    Map<String, Object> item2 = new LinkedHashMap<>();
    item2.put("item_name", "gadget");
    item2.put("quantity", 1);

    Map<String, Object> item3 = new LinkedHashMap<>();
    item3.put("item_name", "thingamajig");
    item3.put("quantity", 3);

    Map<String, Object> order1 = new LinkedHashMap<>();
    order1.put("order_name", "order1");
    order1.put("items", List.of(item1, item2));

    Map<String, Object> order2 = new LinkedHashMap<>();
    order2.put("order_name", "order2");
    order2.put("items", List.of(item3));

    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "parent_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("p1").build())
            .putFields(
                "orders",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(GSON.toJson(order1))
                    .addValue(GSON.toJson(order2))
                    .build())
            .build());

    Map<String, Object> item4 = new LinkedHashMap<>();
    item4.put("item_name", "doohickey");
    item4.put("quantity", 5);

    Map<String, Object> order3 = new LinkedHashMap<>();
    order3.put("order_name", "order3");
    order3.put("items", List.of(item4));

    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "parent_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("p2").build())
            .putFields(
                "orders",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(GSON.toJson(order3))
                    .build())
            .build());

    return docs;
  }

  // ─────────────────────────── script plugin ────────────────────────────────

  private void initScripts() {
    String config = "nodeName: \"server_foo\"";
    ScriptService.initialize(
        new NrtsearchConfig(new ByteArrayInputStream(config.getBytes())),
        List.of(new TestScriptPlugin()));
  }

  static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestScriptEngine());
    }
  }

  static class TestScriptEngine implements ScriptEngine {
    @Override
    public String getLang() {
      return "test_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      ScoreScript.Factory factory = ctx -> new TestScriptFactory(ctx, source);
      return context.factoryClazz.cast(factory);
    }
  }

  static class TestScriptFactory extends ScoreScript.SegmentFactory {
    private final ScriptFactoryContext factoryContext;
    private final String scriptId;

    TestScriptFactory(ScriptFactoryContext factoryContext, String scriptId) {
      super(factoryContext);
      this.factoryContext = factoryContext;
      this.scriptId = scriptId;
    }

    @Override
    public boolean needs_score() {
      return false;
    }

    @Override
    public DoubleValues newInstance(LeafReaderContext ctx, DoubleValues scores) {
      return switch (scriptId) {
        case "item_parent_field_access" ->
            new ItemParentFieldAccessScript(factoryContext, ctx, scores);
        case "item_root_chain_access" -> new ItemRootChainAccessScript(factoryContext, ctx, scores);
        case "root_grandchild_quantity_sum" ->
            new RootGrandchildQuantitySumScript(factoryContext, ctx, scores);
        case "order_level_item_quantity_sum" ->
            new OrderLevelItemQuantitySumScript(factoryContext, ctx, scores);
        default -> throw new IllegalArgumentException("Unknown script: " + scriptId);
      };
    }
  }

  /**
   * From an item doc, accesses _PARENT.orders.order_name (the immediate parent order's name).
   * Returns 1.0 if the field is accessible and non-empty, -1.0 otherwise.
   */
  static class ItemParentFieldAccessScript extends ScoreScript {
    ItemParentFieldAccessScript(
        ScriptFactoryContext ctx, LeafReaderContext leaf, DoubleValues scores) {
      super(ctx, leaf, scores);
    }

    @Override
    public double execute() {
      try {
        Map<String, LoadedDocValues<?>> doc = getDoc();
        LoadedDocValues<?> parentOrderName = doc.get("_PARENT.orders.order_name");
        return (parentOrderName != null && parentOrderName.size() > 0) ? 1.0 : -1.0;
      } catch (Exception e) {
        return -1.0;
      }
    }
  }

  /**
   * From an item doc, accesses _PARENT._PARENT.parent_field (the root doc's parent_field via two
   * hops). Returns 1.0 if the field is accessible and non-empty, -1.0 otherwise.
   */
  static class ItemRootChainAccessScript extends ScoreScript {
    ItemRootChainAccessScript(
        ScriptFactoryContext ctx, LeafReaderContext leaf, DoubleValues scores) {
      super(ctx, leaf, scores);
    }

    @Override
    public double execute() {
      try {
        Map<String, LoadedDocValues<?>> doc = getDoc();
        LoadedDocValues<?> rootParentField = doc.get("_PARENT._PARENT.parent_field");
        return (rootParentField != null && rootParentField.size() > 0) ? 1.0 : -1.0;
      } catch (Exception e) {
        return -1.0;
      }
    }
  }

  /**
   * From a root doc, sums all _CHILDREN.orders.items.quantity values (grandchild aggregation).
   * Returns the sum; a return of 0 with no exception means no grandchildren were found.
   */
  static class RootGrandchildQuantitySumScript extends ScoreScript {
    RootGrandchildQuantitySumScript(
        ScriptFactoryContext ctx, LeafReaderContext leaf, DoubleValues scores) {
      super(ctx, leaf, scores);
    }

    @Override
    public double execute() {
      try {
        Map<String, LoadedDocValues<?>> doc = getDoc();
        LoadedDocValues<?> quantities = doc.get("_CHILDREN.orders.items.quantity");
        if (quantities == null || quantities.size() == 0) {
          return 0.0;
        }
        double sum = 0;
        for (int i = 0; i < quantities.size(); i++) {
          sum += ((Number) quantities.get(i)).doubleValue();
        }
        return sum;
      } catch (Exception e) {
        return -1.0;
      }
    }
  }

  /**
   * From an order doc (queryNestedPath="orders"), sums _CHILDREN.orders.items.quantity. With the
   * correct parentBitSetProducer (orders bitset), this returns the sum of this specific order's
   * items. Without the fix (root bitset), parentBitSet.get(orderDocId) is false and the result is
   * always 0.
   */
  static class OrderLevelItemQuantitySumScript extends ScoreScript {
    OrderLevelItemQuantitySumScript(
        ScriptFactoryContext ctx, LeafReaderContext leaf, DoubleValues scores) {
      super(ctx, leaf, scores);
    }

    @Override
    public double execute() {
      try {
        Map<String, LoadedDocValues<?>> doc = getDoc();
        LoadedDocValues<?> quantities = doc.get("_CHILDREN.orders.items.quantity");
        if (quantities == null || quantities.size() == 0) {
          return 0.0;
        }
        double sum = 0;
        for (int i = 0; i < quantities.size(); i++) {
          sum += ((Number) quantities.get(i)).doubleValue();
        }
        return sum;
      } catch (Exception e) {
        return -1.0;
      }
    }
  }

  // ─────────────────────────── tests ────────────────────────────────────────

  /**
   * Verifies that _PARENT.orders.order_name is accessible from item-level child docs. The offset
   * from each item must point to its immediate parent order (not root), so the field lookup on the
   * parent doc returns a non-empty value.
   */
  @Test
  public void testParentFieldAccessFromItemLevel() {
    initScripts();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(100)
                    .setQuery(functionScoreQuery("item_parent_field_access"))
                    .setQueryNestedPath("orders.items")
                    .build());

    assertTrue("Expected item-level results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "Expected _PARENT.orders.order_name to be accessible from item doc (score=1.0)",
          1.0,
          hit.getScore(),
          0.001);
    }
  }

  /**
   * Verifies that _PARENT._PARENT.parent_field is accessible from item-level child docs via two
   * hops: item → order → root. This exercises the chained _PARENT. prefix resolution and confirms
   * that order docs also have correct _parent_offset values pointing to root.
   */
  @Test
  public void testChainedParentAccessFromItemLevel() {
    initScripts();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(100)
                    .setQuery(functionScoreQuery("item_root_chain_access"))
                    .setQueryNestedPath("orders.items")
                    .build());

    assertTrue("Expected item-level results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "Expected _PARENT._PARENT.parent_field to be accessible from item doc (score=1.0)",
          1.0,
          hit.getScore(),
          0.001);
    }
  }

  /**
   * Verifies that _CHILDREN.orders.items.quantity correctly aggregates grandchild values from root
   * docs. The childPathBitSet filters to _nested_path=orders.items, collecting only item-level docs
   * within the root's block range.
   *
   * <p>Doc 1: widget(qty=2) + gadget(qty=1) + thingamajig(qty=3) = 6 Doc 2: doohickey(qty=5) = 5
   */
  @Test
  public void testGrandchildQuantitySumFromRoot() {
    initScripts();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(functionScoreQuery("root_grandchild_quantity_sum"))
                    .build());

    assertEquals(2, response.getHitsCount());

    double totalScore = 0;
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Expected positive score from grandchild sum, got " + score, score > 0);
      totalScore += score;
    }
    // Total across both docs: (2+1+3) + 5 = 11
    assertEquals("Expected total grandchild quantity sum across both docs", 11.0, totalScore, 0.01);
  }

  /**
   * Verifies that _CHILDREN.orders.items.quantity scopes correctly to each order's own items when
   * the search runs at queryNestedPath="orders". Before the fix, parentBitSet.get() returns false
   * for order docs (they are not root docs), so every score is 0.0. After the fix,
   * parentBitSetProducer = orders bitset, and each order scores the sum of its own items'
   * quantities: order1=3, order2=3, order3=5, total=11.
   */
  @Test
  public void testChildrenAccessFromMidLevelParent() {
    initScripts();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(100)
                    .setQuery(functionScoreQuery("order_level_item_quantity_sum"))
                    .setQueryNestedPath("orders")
                    .build());

    assertEquals("Expected one hit per order", 3, response.getHitsCount());

    double total = 0;
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue(
          "Expected each order to score its items' quantity sum (> 0), got " + score, score > 0);
      total += score;
    }
    assertEquals("order1(2+1=3) + order2(3) + order3(5) = 11", 11.0, total, 0.01);
  }

  /**
   * Verifies that _CHILDREN.orders.items.quantity works correctly when a FunctionScoreQuery script
   * runs on order docs inside a NestedQuery, with no queryNestedPath on the SearchRequest.
   *
   * <p>The SearchRequest has queryNestedPath="" (root level), but the FunctionScoreQuery is
   * evaluated on ORDER documents — the inner query of NestedQuery(path="orders"). The current fix
   * that uses searchRequest.getQueryNestedPath() to pick the parent bitset reads "" → uses root
   * bitset → parentBitSet.get(orderDocId) = false → all order scores = 0.0 → root docs score 0.
   *
   * <p>The correct fix uses the current document's level (determined at setDocId time), finds the
   * orders bitset for order docs, and returns the correct per-order item quantities.
   *
   * <p>Expected: doc1 scores order1(2+1=3) + order2(3) = 6; doc2 scores order3(5) = 5; total = 11.
   */
  @Test
  public void testChildrenAccessInsideNestedQuery() {
    initScripts();

    Query nestedFunctionScore =
        Query.newBuilder()
            .setNestedQuery(
                NestedQuery.newBuilder()
                    .setPath("orders")
                    .setScoreMode(NestedQuery.ScoreMode.SUM)
                    .setQuery(functionScoreQuery("order_level_item_quantity_sum")))
            .build();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(nestedFunctionScore)
                    .build());

    assertEquals("Expected 2 root doc hits", 2, response.getHitsCount());

    double total = 0;
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Expected root doc to score > 0 (sum of order scores), got " + score, score > 0);
      total += score;
    }
    assertEquals("doc1: order1(3)+order2(3)=6, doc2: order3(5)=5, total=11", 11.0, total, 0.01);
  }

  // ────────────────────────── helpers ───────────────────────────────────────

  private Query functionScoreQuery(String scriptId) {
    return Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setScript(Script.newBuilder().setLang("test_lang").setSource(scriptId).build())
                .setQuery(
                    Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()).build())
                .build())
        .build();
  }
}
