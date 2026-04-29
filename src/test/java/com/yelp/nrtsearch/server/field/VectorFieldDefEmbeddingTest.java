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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.embedding.EmbeddingCreator;
import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import com.yelp.nrtsearch.server.embedding.EmbeddingProviderFactory;
import com.yelp.nrtsearch.server.field.VectorFieldDef.FloatVectorFieldDef;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Before;
import org.junit.Test;

public class VectorFieldDefEmbeddingTest {

  private static final float[] MOCK_EMBEDDING = new float[] {0.1f, 0.2f, 0.3f};
  private static final int DIMENSIONS = 3;

  @Before
  public void setUp() {
    // Initialize SimilarityCreator (needed by IndexableFieldDef constructor)
    NrtsearchConfig simConfig =
        new NrtsearchConfig(new ByteArrayInputStream("nodeName: test".getBytes()));
    SimilarityCreator.initialize(simConfig, Collections.emptyList());

    String yaml =
        String.join(
            "\n",
            "nodeName: test",
            "embeddingProviders:",
            "  test-provider:",
            "    type: mock",
            "    dimensions: 3");
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
    Plugin mockPlugin = new MockEmbeddingPlugin();
    EmbeddingCreator.initialize(config, List.of(mockPlugin));
  }

  @Test
  public void testParseVectorFieldWithTextEmbedding() {
    FloatVectorFieldDef fieldDef = createFieldDefWithEmbedding("test-provider");
    Document document = new Document();

    fieldDef.parseVectorField("hello world", document);

    // Should have a BinaryDocValuesField from the embedding
    assertEquals(1, document.getFields().size());
    IndexableField field = document.getFields().get(0);
    assertEquals("test_vector", field.name());
    assertNotNull(field.binaryValue());
  }

  @Test
  public void testParseVectorFieldWithJsonArrayAndEmbeddingProvider() {
    FloatVectorFieldDef fieldDef = createFieldDefWithEmbedding("test-provider");
    Document document = new Document();

    // Even with an embedding provider configured, JSON arrays should use existing parsing
    fieldDef.parseVectorField("[1.0, 2.0, 3.0]", document);

    assertEquals(1, document.getFields().size());
    IndexableField field = document.getFields().get(0);
    assertEquals("test_vector", field.name());
    assertNotNull(field.binaryValue());
  }

  @Test
  public void testParseVectorFieldWithJsonArrayNoProvider() {
    FloatVectorFieldDef fieldDef = createFieldDefWithoutEmbedding();
    Document document = new Document();

    fieldDef.parseVectorField("[1.0, 2.0, 3.0]", document);

    assertEquals(1, document.getFields().size());
    IndexableField field = document.getFields().get(0);
    assertEquals("test_vector", field.name());
    assertNotNull(field.binaryValue());
  }

  @Test(expected = com.google.gson.JsonSyntaxException.class)
  public void testParseVectorFieldWithTextNoProviderFails() {
    FloatVectorFieldDef fieldDef = createFieldDefWithoutEmbedding();
    Document document = new Document();

    // Without an embedding provider, plain text should fail on JSON parsing
    fieldDef.parseVectorField("hello world", document);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseVectorFieldWithNonexistentProviderFails() {
    FloatVectorFieldDef fieldDef = createFieldDefWithEmbedding("nonexistent-provider");
    Document document = new Document();

    fieldDef.parseVectorField("hello world", document);
  }

  @Test
  public void testEmbeddingProviderNameGetter() {
    FloatVectorFieldDef withProvider = createFieldDefWithEmbedding("test-provider");
    assertEquals("test-provider", withProvider.getEmbeddingProviderName());

    FloatVectorFieldDef withoutProvider = createFieldDefWithoutEmbedding();
    assertNull(withoutProvider.getEmbeddingProviderName());
  }

  @Test
  public void testParseVectorFieldWithWhitespaceAroundJsonArray() {
    FloatVectorFieldDef fieldDef = createFieldDefWithEmbedding("test-provider");
    Document document = new Document();

    // Whitespace-padded JSON array should still be parsed as a vector, not text
    fieldDef.parseVectorField("  [1.0, 2.0, 3.0]  ", document);

    assertEquals(1, document.getFields().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmbeddingDimensionMismatchFails() {
    // Create a field with 5 dimensions but provider returns 3-dimensional vectors
    FloatVectorFieldDef fieldDef = createFieldDefWithEmbeddingAndDimensions("test-provider", 5);
    Document document = new Document();

    fieldDef.parseVectorField("hello world", document);
  }

  private FloatVectorFieldDef createFieldDefWithEmbedding(String providerName) {
    return createFieldDefWithEmbeddingAndDimensions(providerName, DIMENSIONS);
  }

  private FloatVectorFieldDef createFieldDefWithEmbeddingAndDimensions(
      String providerName, int dimensions) {
    Field requestField =
        Field.newBuilder()
            .setName("test_vector")
            .setType(FieldType.VECTOR)
            .setStoreDocValues(true)
            .setVectorDimensions(dimensions)
            .setEmbeddingProvider(providerName)
            .build();
    return new FloatVectorFieldDef(
        "test_vector", requestField, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  private FloatVectorFieldDef createFieldDefWithoutEmbedding() {
    Field requestField =
        Field.newBuilder()
            .setName("test_vector")
            .setType(FieldType.VECTOR)
            .setStoreDocValues(true)
            .setVectorDimensions(DIMENSIONS)
            .build();
    return new FloatVectorFieldDef(
        "test_vector", requestField, mock(FieldDefCreator.FieldDefCreatorContext.class));
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
                float[] result = new float[dims];
                // Fill with deterministic values based on text hash
                for (int i = 0; i < dims; i++) {
                  result[i] = MOCK_EMBEDDING[i % MOCK_EMBEDDING.length];
                }
                return result;
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
