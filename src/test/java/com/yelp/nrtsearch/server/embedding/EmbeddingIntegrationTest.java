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
package com.yelp.nrtsearch.server.embedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration test for the embedding feature. Verifies the full flow: register a VECTOR field with
 * embeddingProvider, index text documents (auto-embedded at index time), and search using
 * query_text (auto-embedded at search time).
 */
public class EmbeddingIntegrationTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * Deterministic mock embedding provider that maps specific text strings to known vectors. This
   * allows cosine similarity to meaningfully distinguish results.
   */
  private static final Map<String, float[]> VECTOR_MAP = new HashMap<>();

  static {
    VECTOR_MAP.put("great food", normalize(new float[] {0.9f, 0.1f, 0.1f}));
    VECTOR_MAP.put("terrible service", normalize(new float[] {0.1f, 0.9f, 0.1f}));
    VECTOR_MAP.put("nice atmosphere", normalize(new float[] {0.1f, 0.1f, 0.9f}));
    // Query vectors
    VECTOR_MAP.put("delicious food", normalize(new float[] {0.8f, 0.2f, 0.1f}));
    VECTOR_MAP.put("bad service", normalize(new float[] {0.2f, 0.8f, 0.1f}));
    // Override provider returns a distinct vector
    VECTOR_MAP.put("override_delicious food", normalize(new float[] {0.1f, 0.1f, 0.9f}));
  }

  private static float[] normalize(float[] vec) {
    float mag = 0;
    for (float v : vec) {
      mag += v * v;
    }
    mag = (float) Math.sqrt(mag);
    float[] result = new float[vec.length];
    for (int i = 0; i < vec.length; i++) {
      result[i] = vec[i] / mag;
    }
    return result;
  }

  @Override
  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsVectorEmbedding.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "embedding_field", MultiValuedField.newBuilder().addValue("great food").build())
            .build());
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "embedding_field",
                MultiValuedField.newBuilder().addValue("terrible service").build())
            .build());
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("3").build())
            .putFields(
                "embedding_field",
                MultiValuedField.newBuilder().addValue("nice atmosphere").build())
            .build());
    addDocuments(docs.stream());
  }

  @Override
  protected List<Plugin> getPlugins(NrtsearchConfig configuration) {
    return List.of(new TestMockEmbeddingPlugin());
  }

  @Override
  protected String getExtraConfig() {
    return String.join(
        "\n",
        "embeddingProviders:",
        "  test-mock:",
        "    type: mock",
        "    dimensions: 3",
        "  test-mock-override:",
        "    type: mock-override",
        "    dimensions: 3");
  }

  /**
   * Test 1: Index text documents, search using query_text. "delicious food" should be most similar
   * to "great food" via cosine similarity.
   */
  @Test
  public void testSearchWithQueryText() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields("doc_id")
                    .setStartHit(0)
                    .setTopHits(3)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("embedding_field")
                            .setQueryText("delicious food")
                            .setNumCandidates(10)
                            .setK(3)
                            .build())
                    .build());

    assertTrue("Expected at least 1 hit", response.getHitsCount() > 0);
    // The top result should be doc_id "1" ("great food") because "delicious food" is
    // most similar to "great food" in our mock vector space.
    Hit topHit = response.getHits(0);
    String topDocId = topHit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
    assertEquals("1", topDocId);
  }

  /**
   * Test 2: Index with JSON vector, search with query_vector directly. This verifies that existing
   * vector search behavior still works on a field that has embeddingProvider configured.
   */
  @Test
  public void testSearchWithQueryVector() {
    // Use the normalized vector for "delicious food" directly
    float[] queryVec = VECTOR_MAP.get("delicious food");
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields("doc_id")
                    .setStartHit(0)
                    .setTopHits(3)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("embedding_field")
                            .addQueryVector(queryVec[0])
                            .addQueryVector(queryVec[1])
                            .addQueryVector(queryVec[2])
                            .setNumCandidates(10)
                            .setK(3)
                            .build())
                    .build());

    assertTrue("Expected at least 1 hit", response.getHitsCount() > 0);
    // The top result should be doc_id "1" ("great food")
    Hit topHit = response.getHits(0);
    String topDocId = topHit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
    assertEquals("1", topDocId);
  }

  /**
   * Test 3: Providing both query_text and query_vector should produce an error because they are
   * mutually exclusive.
   */
  @Test
  public void testQueryTextAndQueryVectorMutuallyExclusive() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(DEFAULT_TEST_INDEX)
                  .addRetrieveFields("doc_id")
                  .setStartHit(0)
                  .setTopHits(3)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("embedding_field")
                          .setQueryText("delicious food")
                          .addQueryVector(0.5f)
                          .addQueryVector(0.5f)
                          .addQueryVector(0.5f)
                          .setNumCandidates(10)
                          .setK(3)
                          .build())
                  .build());
      fail("Expected error when both query_text and query_vector are set");
    } catch (StatusRuntimeException e) {
      assertTrue(
          "Error should mention mutual exclusivity", e.getMessage().contains("mutually exclusive"));
    }
  }

  /**
   * Test 4: Search using query_text with an embedding_provider override. The override provider
   * ("test-mock-override") prepends "override_" to the text before lookup, producing a different
   * vector. "delicious food" via the override maps to a vector most similar to "nice atmosphere".
   */
  @Test
  public void testQueryTextWithEmbeddingProviderOverride() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields("doc_id")
                    .setStartHit(0)
                    .setTopHits(3)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("embedding_field")
                            .setQueryText("delicious food")
                            .setEmbeddingProvider("test-mock-override")
                            .setNumCandidates(10)
                            .setK(3)
                            .build())
                    .build());

    assertTrue("Expected at least 1 hit", response.getHitsCount() > 0);
    // With the override provider, "delicious food" is mapped to a vector close to
    // "nice atmosphere" (doc_id "3"), not "great food" (doc_id "1").
    Hit topHit = response.getHits(0);
    String topDocId = topHit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
    assertEquals("3", topDocId);
  }

  /**
   * Mock plugin that registers two embedding provider types: "mock" (direct lookup) and
   * "mock-override" (prepends "override_" to text before lookup).
   */
  public static class TestMockEmbeddingPlugin extends Plugin implements EmbeddingPlugin {
    @Override
    public Map<String, EmbeddingProviderFactory> getEmbeddingProviders() {
      Map<String, EmbeddingProviderFactory> factories = new HashMap<>();

      // Standard mock provider: looks up text directly in VECTOR_MAP
      factories.put(
          "mock",
          config -> {
            int dims = ((Number) config.get("dimensions")).intValue();
            return new EmbeddingProvider() {
              @Override
              public float[] embed(String text) {
                float[] vec = VECTOR_MAP.get(text);
                if (vec != null) {
                  return vec;
                }
                // Fallback: return a zero-like vector for unknown text
                return new float[dims];
              }

              @Override
              public int dimensions() {
                return dims;
              }
            };
          });

      // Override mock provider: prepends "override_" to text before lookup
      factories.put(
          "mock-override",
          config -> {
            int dims = ((Number) config.get("dimensions")).intValue();
            return new EmbeddingProvider() {
              @Override
              public float[] embed(String text) {
                float[] vec = VECTOR_MAP.get("override_" + text);
                if (vec != null) {
                  return vec;
                }
                return new float[dims];
              }

              @Override
              public int dimensions() {
                return dims;
              }
            };
          });

      return factories;
    }
  }
}
