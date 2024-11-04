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

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.SimilarityPlugin;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Class to handle the creation of {@link Similarity} instances. Type strings are mapped to {@link
 * SimilarityProvider}s to produce concrete {@link Similarity}s.
 */
public class SimilarityCreator {
  public static final Similarity DEFAULT_SIMILARITY = new BM25Similarity();
  private static SimilarityCreator instance;

  private final Map<String, SimilarityProvider<? extends Similarity>> similarityMap =
      new HashMap<>();

  public SimilarityCreator(NrtsearchConfig configuration) {
    register("classic", params -> new ClassicSimilarity());
    register("BM25", params -> new BM25Similarity());
    register("boolean", params -> new BooleanSimilarity());
  }

  /**
   * Get a {@link Similarity} implementation by name, and with the given parameters. Valid names are
   * any standard types ('classic', 'BM25', 'boolean') or any custom type registered by a {@link
   * SimilarityPlugin}. If name is an empty String, the default BM25 similarity is used.
   *
   * @param name similarity implementation name
   * @param params similarity parameters
   * @return similarity instance
   */
  public Similarity createSimilarity(String name, Map<String, Object> params) {
    if (name.isEmpty()) {
      return DEFAULT_SIMILARITY;
    }
    SimilarityProvider<?> provider = similarityMap.get(name);
    if (provider == null) {
      throw new IllegalArgumentException(
          "Invalid similarity name: " + name + ", must be one of: " + similarityMap.keySet());
    }
    return provider.get(params);
  }

  private void register(Map<String, SimilarityProvider<? extends Similarity>> similarities) {
    similarities.forEach(this::register);
  }

  private void register(String name, SimilarityProvider<? extends Similarity> similarity) {
    if (similarityMap.containsKey(name)) {
      throw new IllegalArgumentException("Similarity " + name + " already exists");
    }
    similarityMap.put(name, similarity);
  }

  /**
   * Initialize singleton instance of {@link SimilarityCreator}. Registers all the standard
   * similarity implementations and any additional provided by {@link SimilarityPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new SimilarityCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof SimilarityPlugin similarityPlugin) {
        instance.register(similarityPlugin.getSimilarities());
      }
    }
  }

  /** Get singleton instance. */
  public static SimilarityCreator getInstance() {
    return instance;
  }
}
