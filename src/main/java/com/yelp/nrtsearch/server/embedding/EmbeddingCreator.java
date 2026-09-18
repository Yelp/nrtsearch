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

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registry for named {@link EmbeddingProvider} instances, collected from plugins. */
public class EmbeddingCreator {
  private static final Logger logger = LoggerFactory.getLogger(EmbeddingCreator.class);
  private static EmbeddingCreator instance;

  private final Map<String, EmbeddingProvider> providerMap = new HashMap<>();

  private EmbeddingCreator() {}

  /**
   * Get a named {@link EmbeddingProvider} instance.
   *
   * @param name provider instance name
   * @return the provider, or null if not found
   */
  public EmbeddingProvider getProvider(String name) {
    return providerMap.get(name);
  }

  private void registerProvider(String name, EmbeddingProvider provider) {
    if (providerMap.containsKey(name)) {
      throw new IllegalArgumentException("Embedding provider already registered: " + name);
    }
    providerMap.put(name, provider);
    logger.info(
        "Registered embedding provider '{}' with {} dimensions", name, provider.dimensions());
  }

  private void registerProviders(Map<String, EmbeddingProvider> providers) {
    providers.forEach(this::registerProvider);
  }

  /**
   * Initialize singleton instance of {@link EmbeddingCreator}. Collects initialized {@link
   * EmbeddingProvider} instances from {@link EmbeddingPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    if (instance != null) {
      instance.closeProviders();
    }
    instance = new EmbeddingCreator();
    for (Plugin plugin : plugins) {
      if (plugin instanceof EmbeddingPlugin embeddingPlugin) {
        instance.registerProviders(embeddingPlugin.getEmbeddingProviders());
      }
    }
  }

  private void closeProviders() {
    for (Map.Entry<String, EmbeddingProvider> entry : providerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
        logger.warn("Error closing embedding provider '{}'", entry.getKey(), e);
      }
    }
  }

  /** Get singleton instance. */
  public static EmbeddingCreator getInstance() {
    return instance;
  }
}
