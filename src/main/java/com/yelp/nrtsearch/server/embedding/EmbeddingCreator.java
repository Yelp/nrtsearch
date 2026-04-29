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

/** Registry for named {@link EmbeddingProvider} instances, initialized from server config. */
public class EmbeddingCreator {
  private static final Logger logger = LoggerFactory.getLogger(EmbeddingCreator.class);
  private static EmbeddingCreator instance;

  private final Map<String, EmbeddingProviderFactory> factoryMap = new HashMap<>();
  private final Map<String, EmbeddingProvider> providerMap = new HashMap<>();

  public EmbeddingCreator(NrtsearchConfig configuration) {}

  /**
   * Get a named {@link EmbeddingProvider} instance.
   *
   * @param name provider instance name from config
   * @return the provider, or null if not found
   */
  public EmbeddingProvider getProvider(String name) {
    return providerMap.get(name);
  }

  private void registerType(String typeName, EmbeddingProviderFactory factory) {
    if (factoryMap.containsKey(typeName)) {
      throw new IllegalArgumentException("Embedding provider type already registered: " + typeName);
    }
    factoryMap.put(typeName, factory);
  }

  private void registerTypes(Map<String, EmbeddingProviderFactory> factories) {
    factories.forEach(this::registerType);
  }

  private void createProviders(NrtsearchConfig configuration) {
    Map<String, Map<String, Object>> providerConfigs = configuration.getEmbeddingProviderConfigs();
    for (Map.Entry<String, Map<String, Object>> entry : providerConfigs.entrySet()) {
      String name = entry.getKey();
      Map<String, Object> config = entry.getValue();
      String type = (String) config.get("type");
      if (type == null) {
        throw new IllegalArgumentException(
            "Embedding provider '" + name + "' missing required 'type' field");
      }
      EmbeddingProviderFactory factory = factoryMap.get(type);
      if (factory == null) {
        throw new IllegalArgumentException(
            "Unknown embedding provider type: "
                + type
                + ", available types: "
                + factoryMap.keySet());
      }
      EmbeddingProvider provider = factory.create(config);
      providerMap.put(name, provider);
      logger.info(
          "Created embedding provider '{}' of type '{}' with {} dimensions",
          name,
          type,
          provider.dimensions());
    }
  }

  /**
   * Initialize singleton instance of {@link EmbeddingCreator}. Registers any additional {@link
   * EmbeddingProvider} type factories provided by {@link EmbeddingPlugin}s, then instantiates named
   * providers from server config.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new EmbeddingCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof EmbeddingPlugin embeddingPlugin) {
        instance.registerTypes(embeddingPlugin.getEmbeddingProviders());
      }
    }
    instance.createProviders(configuration);
  }

  /** Get singleton instance. */
  public static EmbeddingCreator getInstance() {
    return instance;
  }
}
