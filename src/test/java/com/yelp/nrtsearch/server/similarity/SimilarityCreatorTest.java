/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.similarity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.SimilarityPlugin;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.Before;
import org.junit.Test;

public class SimilarityCreatorTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    SimilarityCreator.initialize(getEmptyConfig(), plugins);
  }

  private NrtsearchConfig getEmptyConfig() {
    String config = "nodeName: \"server_foo\"";
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  public static class TestSimilarityPlugin extends Plugin implements SimilarityPlugin {
    @Override
    public Map<String, SimilarityProvider<? extends Similarity>> getSimilarities() {
      Map<String, SimilarityProvider<? extends Similarity>> similarityProviderMap = new HashMap<>();
      similarityProviderMap.put("plugin_similarity", CustomSimilarity::new);
      return similarityProviderMap;
    }

    public static class CustomSimilarity extends Similarity {
      public final Map<String, Object> params;

      public CustomSimilarity(Map<String, Object> params) {
        this.params = params;
      }

      @Override
      public long computeNorm(FieldInvertState state) {
        return 0;
      }

      @Override
      public SimScorer scorer(
          float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return null;
      }
    }
  }

  @Test
  public void testBuiltInSimilarity() {
    Similarity similarity =
        SimilarityCreator.getInstance().createSimilarity("classic", Collections.emptyMap());
    assertTrue(similarity instanceof ClassicSimilarity);
    similarity = SimilarityCreator.getInstance().createSimilarity("BM25", Collections.emptyMap());
    assertTrue(similarity instanceof BM25Similarity);
    similarity =
        SimilarityCreator.getInstance().createSimilarity("boolean", Collections.emptyMap());
    assertTrue(similarity instanceof BooleanSimilarity);
  }

  @Test
  public void testDefaultSimilarity() {
    Similarity similarity =
        SimilarityCreator.getInstance().createSimilarity("", Collections.emptyMap());
    assertTrue(similarity instanceof BM25Similarity);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPluginSimilarityNotDefined() {
    SimilarityCreator.getInstance().createSimilarity("plugin_similarity", Collections.emptyMap());
  }

  @Test
  public void testPluginProvidesSimilarity() {
    init(Collections.singletonList(new TestSimilarityPlugin()));
    Similarity similarity =
        SimilarityCreator.getInstance()
            .createSimilarity("plugin_similarity", Collections.emptyMap());
    assertTrue(similarity instanceof TestSimilarityPlugin.CustomSimilarity);
  }

  @Test
  public void testSimilarityParams() {
    init(Collections.singletonList(new TestSimilarityPlugin()));
    Map<String, Object> params = new HashMap<>();
    params.put("p1", 100);
    params.put("p2", "param2");
    Similarity similarity =
        SimilarityCreator.getInstance().createSimilarity("plugin_similarity", params);
    assertTrue(similarity instanceof TestSimilarityPlugin.CustomSimilarity);
    assertEquals(params, ((TestSimilarityPlugin.CustomSimilarity) similarity).params);
  }
}
