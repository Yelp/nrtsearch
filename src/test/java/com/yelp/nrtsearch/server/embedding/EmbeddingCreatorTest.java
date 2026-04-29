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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class EmbeddingCreatorTest {

  @Before
  public void setUp() {
    EmbeddingCreator.initialize(getEmptyConfig(), Collections.emptyList());
  }

  @Test
  public void testGetProviderFromConfig() {
    String yaml =
        String.join(
            "\n",
            "nodeName: test",
            "embeddingProviders:",
            "  test-provider:",
            "    type: mock",
            "    dimensions: 3");
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
    Plugin mockPlugin = new MockEmbeddingPlugin(3);
    EmbeddingCreator.initialize(config, List.of(mockPlugin));

    EmbeddingProvider provider = EmbeddingCreator.getInstance().getProvider("test-provider");
    assertNotNull(provider);
    assertEquals(3, provider.dimensions());
    float[] result = provider.embed("hello");
    assertEquals(3, result.length);
  }

  @Test
  public void testGetProviderNotFound() {
    EmbeddingCreator.initialize(getEmptyConfig(), Collections.emptyList());
    assertNull(EmbeddingCreator.getInstance().getProvider("nonexistent"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateProviderType() {
    String yaml =
        String.join(
            "\n",
            "nodeName: test",
            "embeddingProviders:",
            "  p1:",
            "    type: mock",
            "    dimensions: 3");
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
    Plugin plugin1 = new MockEmbeddingPlugin(3);
    Plugin plugin2 = new MockEmbeddingPlugin(3);
    EmbeddingCreator.initialize(config, List.of(plugin1, plugin2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownProviderType() {
    String yaml =
        String.join(
            "\n",
            "nodeName: test",
            "embeddingProviders:",
            "  test-provider:",
            "    type: unknown-type",
            "    dimensions: 3");
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
    EmbeddingCreator.initialize(config, Collections.emptyList());
  }

  private NrtsearchConfig getEmptyConfig() {
    return new NrtsearchConfig(new ByteArrayInputStream("nodeName: test".getBytes()));
  }

  private static class MockEmbeddingPlugin extends Plugin implements EmbeddingPlugin {
    private final int dimensions;

    MockEmbeddingPlugin(int dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public Map<String, EmbeddingProviderFactory> getEmbeddingProviders() {
      return Map.of(
          "mock",
          config -> {
            int dims = ((Number) config.get("dimensions")).intValue();
            return new EmbeddingProvider() {
              @Override
              public float[] embed(String text) {
                return new float[dims];
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
