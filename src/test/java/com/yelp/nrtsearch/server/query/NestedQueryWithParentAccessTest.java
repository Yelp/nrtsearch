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
package com.yelp.nrtsearch.server.query;

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

public class NestedQueryWithParentAccessTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final List<String> RETRIEVE_LIST =
      Arrays.asList("doc_id", "int_score", "int_field");

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
    return getFieldsFromResourceFile("/registerFieldsNestedQueryWithParentAccess.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    List<AddDocumentRequest> requestChunk = new ArrayList<>();

    for (int id = 0; id < NUM_DOCS; ++id) {
      Map<String, Object> pickup1 = new HashMap<>();
      pickup1.put("name", "AAA");
      pickup1.put("hours", List.of(id));

      Map<String, Object> pickup2 = new HashMap<>();
      pickup2.put("name", "BBB");
      pickup2.put("hours", List.of(id + 1));
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "sorted_doc_values_facet_field",
                  AddDocumentRequest.MultiValuedField.newBuilder().addValue("0").build())
              .putFields(
                  "pickup_partners",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(gson.toJson(pickup1))
                      .addValue(gson.toJson(pickup2))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Arrays.asList(new TestScriptEngine());
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
      if ("parent_access_test".equals(scriptId)) {
        return new ParentAccessDoubleScoreScript(params, docLookup, ctx, scores);
      } else {
        return new DoubleScoreScript(params, docLookup, ctx, scores);
      }
    }

    public DocLookup getDocLookup() {
      return docLookup;
    }
  }

  static class DoubleScoreScript extends ScoreScript {
    public DoubleScoreScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      Map<String, LoadedDocValues<?>> segmentDocLookup = this.getDoc();
      LoadedDocValues<?> loadedDocValues = segmentDocLookup.get("pickup_partners.hours");
      Object value = loadedDocValues.get(0);
      return value instanceof Integer ? ((Integer) value).doubleValue() : (double) value;
    }
  }

  @Test
  public void testNestedQueryWithParentAccess() throws IOException {
    ParentAccessDoubleScoreScript.resetExecutionCount();
    init(Collections.singletonList(new TestScriptPlugin()));

    Script script =
        Script.newBuilder().setLang("test_lang").setSource("parent_access_test").build();

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
            .addAllRetrieveFields(RETRIEVE_LIST)
            .setQuery(functionScoreQuery)
            .setQueryNestedPath("pickup_partners")
            .build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(request);

    assertTrue(
        "The ParentAccessDoubleScoreScript.execute() method should be called during search",
        ParentAccessDoubleScoreScript.wasExecuted());

    assertTrue("Search should return results", response.getHitsCount() > 0);

    for (SearchResponse.Hit hit : response.getHitsList()) {
      double score = hit.getScore();

      if (score == -1.0) {
        throw new AssertionError("Failed to access nested field 'pickup_partners.hours'");
      } else if (score == -2.0) {
        throw new AssertionError("Failed to access parent field 'int_score'");
      } else if (score == -3.0) {
        throw new AssertionError("Nested field value is not an Integer");
      } else if (score == -4.0) {
        throw new AssertionError("Parent field value is not an Integer");
      } else if (score == -5.0) {
        throw new AssertionError("Exception occurred during parent field access");
      } else if (score == -6.0) {
        throw new AssertionError("Failed to access parent field 'int_score' via _PARENT. notation");
      } else if (score == -7.0) {
        throw new AssertionError(
            "Parent field values accessed by int_score and _PARENT.int_score do not match");
      }

      // If we get here, the score should be positive (successful parent access, and child access)
      assertTrue(
          "Score should be positive, indicating successful parent field access. Got: " + score,
          score > 0.0);
    }

    // The main validation is that we got positive scores from the script
    // which means both nested and parent field access worked correctly.
    // We don't need to validate the individual field values from the search response
  }

  static class ParentAccessDoubleScoreScript extends ScoreScript {
    private static int executionCount = 0;

    public ParentAccessDoubleScoreScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      executionCount++;

      Map<String, LoadedDocValues<?>> segmentDocLookup = this.getDoc();

      try {
        LoadedDocValues<?> nestedField = segmentDocLookup.get("pickup_partners.hours");
        if (nestedField == null) {
          return -1.0; // Negative score indicates failure
        }

        Object nestedValue = nestedField.getFirst();

        // Test both automatic parent field access and explicit _PARENT. notation
        LoadedDocValues<?> parentField = segmentDocLookup.get("int_score");
        LoadedDocValues<?> explicitParentField = segmentDocLookup.get("_PARENT.int_score");

        if (parentField == null) {
          return -2.0; // Different negative score for different failure type
        }

        if (explicitParentField == null) {
          return -6.0; // Failure to access via _PARENT. notation
        }

        Object parentValue = parentField.getFirst();
        Object explicitParentValue = explicitParentField.getFirst();

        // Verify both methods return the same value
        if (!parentValue.equals(explicitParentValue)) {
          return -7.0; // Values don't match
        }

        // Combine the values to show we can access both
        return (nestedValue instanceof Integer ? ((Integer) nestedValue).doubleValue() : -3.0)
            + (parentValue instanceof Integer ? ((Integer) parentValue).doubleValue() : -4.0);
      } catch (Exception e) {
        return -5.0; // Negative score on error to fail the test
      }
    }

    public static boolean wasExecuted() {
      return executionCount > 0;
    }

    public static void resetExecutionCount() {
      executionCount = 0;
    }
  }
}
