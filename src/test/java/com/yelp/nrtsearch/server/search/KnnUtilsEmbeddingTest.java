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

import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.embedding.EmbeddingCreator;
import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import com.yelp.nrtsearch.server.embedding.EmbeddingProviderFactory;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KnnUtilsEmbeddingTest {

  private static final float[] MOCK_VECTOR = {0.1f, 0.2f, 0.3f};

  @Before
  public void setUp() {
    // Initialize EmbeddingCreator with a mock provider for each test
    String yaml =
        String.join(
            "\n",
            "nodeName: test",
            "embeddingProviders:",
            "  test-provider:",
            "    type: mock",
            "    dimensions: 3",
            "  alt-provider:",
            "    type: mock",
            "    dimensions: 3");
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
    EmbeddingCreator.initialize(config, List.of(new MockEmbeddingPlugin()));
  }

  @Test
  public void testNoQueryTextPassthrough() {
    // KnnQuery with no query_text should be returned as-is
    KnnQuery knnQuery = KnnQuery.newBuilder().setField("vector_field").addQueryVector(1.0f).build();

    VectorFieldDef<?> fieldDef = mockFieldDef(null);
    KnnQuery result = KnnUtils.resolveQueryText(knnQuery, fieldDef);
    assertSame(knnQuery, result);
  }

  @Test
  public void testQueryTextWithQueryVectorThrows() {
    // query_text + query_vector both set should throw
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector_field")
            .setQueryText("hello world")
            .addQueryVector(1.0f)
            .build();

    VectorFieldDef<?> fieldDef = mockFieldDef("test-provider");
    try {
      KnnUtils.resolveQueryText(knnQuery, fieldDef);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("mutually exclusive"));
    }
  }

  @Test
  public void testQueryTextWithQueryByteVectorThrows() {
    // query_text + query_byte_vector both set should throw
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector_field")
            .setQueryText("hello world")
            .setQueryByteVector(ByteString.copyFrom(new byte[] {1, 2, 3}))
            .build();

    VectorFieldDef<?> fieldDef = mockFieldDef("test-provider");
    try {
      KnnUtils.resolveQueryText(knnQuery, fieldDef);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("mutually exclusive"));
    }
  }

  @Test
  public void testQueryTextNoProviderThrows() {
    // query_text set, but no provider configured on field and no override
    KnnQuery knnQuery =
        KnnQuery.newBuilder().setField("vector_field").setQueryText("hello world").build();

    VectorFieldDef<?> fieldDef = mockFieldDef(null);
    try {
      KnnUtils.resolveQueryText(knnQuery, fieldDef);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("query_text requires an embedding provider"));
      assertTrue(e.getMessage().contains("vector_field"));
    }
  }

  @Test
  public void testQueryTextEmptyProviderThrows() {
    // query_text set, field returns empty string for provider
    KnnQuery knnQuery =
        KnnQuery.newBuilder().setField("vector_field").setQueryText("hello world").build();

    VectorFieldDef<?> fieldDef = mockFieldDef("");
    try {
      KnnUtils.resolveQueryText(knnQuery, fieldDef);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("query_text requires an embedding provider"));
    }
  }

  @Test
  public void testQueryTextProviderNotFoundThrows() {
    // query_text set, field has provider name that doesn't exist
    KnnQuery knnQuery =
        KnnQuery.newBuilder().setField("vector_field").setQueryText("hello world").build();

    VectorFieldDef<?> fieldDef = mockFieldDef("nonexistent-provider");
    try {
      KnnUtils.resolveQueryText(knnQuery, fieldDef);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Embedding provider not found"));
      assertTrue(e.getMessage().contains("nonexistent-provider"));
    }
  }

  @Test
  public void testQueryTextResolvedViaFieldProvider() {
    // query_text set, field has configured provider
    KnnQuery knnQuery =
        KnnQuery.newBuilder().setField("vector_field").setQueryText("hello world").build();

    VectorFieldDef<?> fieldDef = mockFieldDef("test-provider");
    KnnQuery result = KnnUtils.resolveQueryText(knnQuery, fieldDef);

    // Verify query_text was cleared
    assertTrue(result.getQueryText().isEmpty());
    // Verify query_vector was populated
    assertEquals(MOCK_VECTOR.length, result.getQueryVectorCount());
    for (int i = 0; i < MOCK_VECTOR.length; i++) {
      assertEquals(MOCK_VECTOR[i], result.getQueryVector(i), 0.0001f);
    }
    // Verify other fields preserved
    assertEquals("vector_field", result.getField());
  }

  @Test
  public void testQueryTextWithEmbeddingProviderOverride() {
    // query_text set with explicit embedding_provider override
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector_field")
            .setQueryText("hello world")
            .setEmbeddingProvider("alt-provider")
            .build();

    // Field has a different provider, but override should take precedence
    VectorFieldDef<?> fieldDef = mockFieldDef("test-provider");
    KnnQuery result = KnnUtils.resolveQueryText(knnQuery, fieldDef);

    // Verify query_text was cleared and vector was populated
    assertTrue(result.getQueryText().isEmpty());
    assertEquals(MOCK_VECTOR.length, result.getQueryVectorCount());
    for (int i = 0; i < MOCK_VECTOR.length; i++) {
      assertEquals(MOCK_VECTOR[i], result.getQueryVector(i), 0.0001f);
    }
  }

  @Test
  public void testQueryTextOverrideUsedWhenFieldHasNoProvider() {
    // Field has no provider, but query has an override
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector_field")
            .setQueryText("hello world")
            .setEmbeddingProvider("test-provider")
            .build();

    VectorFieldDef<?> fieldDef = mockFieldDef(null);
    KnnQuery result = KnnUtils.resolveQueryText(knnQuery, fieldDef);

    assertTrue(result.getQueryText().isEmpty());
    assertEquals(MOCK_VECTOR.length, result.getQueryVectorCount());
  }

  @Test
  public void testQueryTextPreservesOtherFields() {
    // Verify that other KnnQuery fields are preserved after resolution
    KnnQuery knnQuery =
        KnnQuery.newBuilder()
            .setField("vector_field")
            .setQueryText("hello world")
            .setK(10)
            .setNumCandidates(100)
            .build();

    VectorFieldDef<?> fieldDef = mockFieldDef("test-provider");
    KnnQuery result = KnnUtils.resolveQueryText(knnQuery, fieldDef);

    assertEquals("vector_field", result.getField());
    assertEquals(10, result.getK());
    assertEquals(100, result.getNumCandidates());
    assertEquals(MOCK_VECTOR.length, result.getQueryVectorCount());
  }

  private VectorFieldDef<?> mockFieldDef(String embeddingProviderName) {
    VectorFieldDef<?> fieldDef = Mockito.mock(VectorFieldDef.class);
    Mockito.when(fieldDef.getEmbeddingProviderName()).thenReturn(embeddingProviderName);
    return fieldDef;
  }

  private static class MockEmbeddingPlugin extends Plugin implements EmbeddingPlugin {
    @Override
    public Map<String, EmbeddingProviderFactory> getEmbeddingProviders() {
      return Map.of(
          "mock",
          config -> {
            int dims = ((Number) config.get("dimensions")).intValue();
            return new EmbeddingProvider() {
              @Override
              public float[] embed(String text) {
                // Return a known vector for testing
                return MOCK_VECTOR.clone();
              }

              @Override
              public int dimensions() {
                return dims;
              }
            };
          });
    }
  }
}
