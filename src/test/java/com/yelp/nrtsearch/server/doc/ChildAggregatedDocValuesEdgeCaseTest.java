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
 * Tests edge cases for {@link ChildAggregatedDocValues} that are not covered by the path filter
 * test:
 *
 * <ul>
 *   <li>DocValues object reuse across multiple documents in the same segment (childrenLoaderCache)
 *   <li>Parent with zero children (childCount <= 0 early return in setDocId)
 *   <li>lastParentDocId caching guard (same docId called twice returns cached values)
 *   <li>_CHILDREN access on non-parent documents returns empty
 * </ul>
 *
 * <p>Document layout: docs 0-7 have 2 appointments each, docs 8-9 have no nested children. All
 * documents are in a single segment so the childrenLoaderCache is exercised across multiple parent
 * documents.
 */
public class ChildAggregatedDocValuesEdgeCaseTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index_child_edge";
  private static final int NUM_DOCS_WITH_CHILDREN = 8;
  private static final int NUM_DOCS_WITHOUT_CHILDREN = 2;
  private static final int TOTAL_DOCS = NUM_DOCS_WITH_CHILDREN + NUM_DOCS_WITHOUT_CHILDREN;

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
    return getFieldsFromResourceFile("/registerFieldsChildAggDocValuesEdgeCaseTest.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    List<AddDocumentRequest> requestChunk = new ArrayList<>();

    for (int id = 0; id < TOTAL_DOCS; ++id) {
      AddDocumentRequest.Builder docBuilder =
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
                      .build());

      if (id < NUM_DOCS_WITH_CHILDREN) {
        Map<String, Object> appt1 = new HashMap<>();
        appt1.put("price", id * 100);
        appt1.put("duration", id * 10);

        Map<String, Object> appt2 = new HashMap<>();
        appt2.put("price", id * 100 + 50);
        appt2.put("duration", id * 10 + 5);

        Map<String, Object> review1 = new HashMap<>();
        review1.put("stars", id % 5 + 1);
        review1.put("helpful", id * 2);

        Map<String, Object> review2 = new HashMap<>();
        review2.put("stars", (id + 1) % 5 + 1);
        review2.put("helpful", id * 2 + 1);

        docBuilder
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
                    .build());
      }
      // docs 8-9: no appointments or reviews fields — zero children

      requestChunk.add(docBuilder.build());
    }

    // Add all docs in one batch to ensure they land in a single segment
    addDocuments(requestChunk.stream());
    writer.commit();
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
        case "docvalues_reuse_test" -> new DocValuesReuseScript(params, docLookup, ctx, scores);
        case "zero_children_test" -> new ZeroChildrenScript(params, docLookup, ctx, scores);
        case "set_doc_id_twice_test" -> new SetDocIdTwiceScript(params, docLookup, ctx, scores);
        default -> throw new IllegalArgumentException("Unknown script: " + scriptId);
      };
    }
  }

  // ---- Script implementations ----

  /**
   * Verifies that calling doc.get("_CHILDREN.appointments.price") twice in the same execute()
   * returns the same object (proving the childrenLoaderCache in SegmentDocLookup works). Also
   * implicitly tests that the ChildAggregatedDocValues instance is reused across multiple documents
   * within the same segment, since this script executes for every matching doc.
   *
   * <p>Returns: 1.0 if same instance, error codes otherwise.
   */
  static class DocValuesReuseScript extends ScoreScript {
    private LoadedDocValues<?> previousReference = null;

    public DocValuesReuseScript(
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
        LoadedDocValues<?> first = doc.get("_CHILDREN.appointments.price");
        LoadedDocValues<?> second = doc.get("_CHILDREN.appointments.price");
        if (first == null || second == null) return -1.0;

        // Same object within a single execute() call — tests childrenLoaderCache
        if (first != second) return -2.0;

        // Across documents in the same segment, the SegmentDocLookup caches
        // ChildAggregatedDocValues per field. Verify by checking that the reference
        // from the previous document's execute() is the same object.
        if (previousReference != null && previousReference != first) return -3.0;
        previousReference = first;

        return 1.0;
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  /**
   * Tests the zero-children path: documents 8-9 have no nested children, so
   * _CHILDREN.appointments.price should return empty (size 0).
   *
   * <p>Returns: for docs with children, the appointment count (should be 2). For docs without
   * children, returns 0. Encodes as: docId * 100 + childCount, allowing us to verify per-doc.
   */
  static class ZeroChildrenScript extends ScoreScript {
    public ZeroChildrenScript(
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
        LoadedDocValues<?> docIdValues = doc.get("int_score");
        if (docIdValues == null || docIdValues.isEmpty()) return -1.0;
        int docId = ((Number) docIdValues.get(0)).intValue();

        LoadedDocValues<?> prices = doc.get("_CHILDREN.appointments.price");
        if (prices == null) return -2.0;

        return docId * 100.0 + prices.size();
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  /**
   * Tests the lastParentDocId caching guard by accessing _CHILDREN.appointments.price twice for the
   * same document. On the second access, setDocId is called again with the same ID, which should
   * hit the early-return cache path.
   *
   * <p>Verifies that the values are identical on both accesses and that the size is consistent.
   * Returns: 1.0 if values match, error codes otherwise.
   */
  static class SetDocIdTwiceScript extends ScoreScript {
    public SetDocIdTwiceScript(
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
        // First access: triggers full child value collection
        LoadedDocValues<?> prices1 = doc.get("_CHILDREN.appointments.price");
        if (prices1 == null) return -1.0;
        int size1 = prices1.size();
        List<Object> values1 = new ArrayList<>();
        for (int i = 0; i < size1; i++) {
          values1.add(prices1.get(i));
        }

        // Second access: same field, same docId — hits lastParentDocId cache
        LoadedDocValues<?> prices2 = doc.get("_CHILDREN.appointments.price");
        if (prices2 == null) return -2.0;
        int size2 = prices2.size();

        if (size1 != size2) return -3.0;

        for (int i = 0; i < size1; i++) {
          if (!prices1.get(i).equals(prices2.get(i))) return -4.0;
        }

        // Also test with a different field to verify independent caching
        LoadedDocValues<?> stars1 = doc.get("_CHILDREN.reviews.stars");
        LoadedDocValues<?> stars2 = doc.get("_CHILDREN.reviews.stars");
        if (stars1 != stars2) return -5.0;

        return 1.0;
      } catch (Exception e) {
        return -99.0;
      }
    }
  }

  // ---- Test methods ----

  /**
   * Verify that the ChildAggregatedDocValues instance is reused across multiple documents in the
   * same segment. The script checks object identity (==) of the LoadedDocValues returned by
   * doc.get("_CHILDREN.appointments.price") both within a single execute() call and across
   * consecutive documents.
   */
  @Test
  public void testDocValuesReuseAcrossDocumentsInSameSegment() {
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("docvalues_reuse_test");

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "DocValues should be the same cached instance across calls. "
              + "Score -2 = different within one execute(), "
              + "-3 = different across documents in segment. Got: "
              + hit.getScore(),
          1.0,
          hit.getScore(),
          0.0001);
    }
  }

  /**
   * Verify that parents with zero children return empty doc values (size 0) when accessing
   * _CHILDREN fields. Documents 8-9 have no nested children, exercising the childCount <= 0
   * early-return path in ChildAggregatedDocValues.setDocId.
   */
  @Test
  public void testParentWithZeroChildrenReturnsEmpty() {
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("zero_children_test");

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      assertTrue("Score should be non-negative (no errors). Got: " + score, score >= 0.0);

      int docId = (int) (score / 100.0);
      int childCount = (int) (score % 100.0);

      if (docId >= NUM_DOCS_WITH_CHILDREN) {
        assertEquals(
            "Doc " + docId + " has no nested children, should have 0 child values", 0, childCount);
      } else {
        assertEquals("Doc " + docId + " should have 2 appointment children", 2, childCount);
      }
    }
  }

  /**
   * Verify the lastParentDocId caching guard: calling setDocId twice with the same doc ID returns
   * the same cached values without re-collection. The script accesses _CHILDREN.appointments.price
   * twice for each document and verifies identical results.
   */
  @Test
  public void testLastParentDocIdCachingGuard() {
    init(Collections.singletonList(new TestScriptPlugin()));

    SearchResponse response = runScriptAtRootLevel("set_doc_id_twice_test");

    assertTrue("Search should return results", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(
          "Values from two accesses with the same docId should be identical. "
              + "Score -3 = size mismatch, -4 = value mismatch, -5 = different stars instance. "
              + "Got: "
              + hit.getScore(),
          1.0,
          hit.getScore(),
          0.0001);
    }
  }

  /**
   * Verify that _CHILDREN access on non-parent documents (child documents queried at nested level)
   * returns empty values, since child documents are not in the parent BitSet.
   */
  @Test
  public void testChildrenAccessOnNonParentDocReturnsEmpty() {
    init(Collections.singletonList(new TestScriptPlugin()));

    Script script =
        Script.newBuilder().setLang("test_lang").setSource("zero_children_test").build();

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

    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(TOTAL_DOCS * 5)
            .addAllRetrieveFields(Arrays.asList("doc_id", "int_score"))
            .setQuery(functionScoreQuery)
            .setQueryNestedPath("appointments")
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertTrue("Search should return nested (child) documents", response.getHitsCount() > 0);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();
      int childCount = (int) (score % 100.0);
      assertEquals(
          "Non-parent documents should have 0 children when accessing _CHILDREN. "
              + "Got score: "
              + score,
          0,
          childCount);
    }
  }

  // ---- Helper ----

  private SearchResponse runScriptAtRootLevel(String scriptSource) {
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

    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(TOTAL_DOCS)
            .addAllRetrieveFields(Arrays.asList("doc_id", "int_score", "business_name"))
            .setQuery(functionScoreQuery)
            .build();

    return getGrpcServer().getBlockingStub().search(request);
  }
}
