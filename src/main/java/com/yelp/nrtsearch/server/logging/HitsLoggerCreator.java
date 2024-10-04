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
package com.yelp.nrtsearch.server.logging;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/** Factory class that handles registration and creation of {@link HitsLogger}s. */
public class HitsLoggerCreator {
  private static HitsLoggerCreator instance;
  private final Map<String, HitsLoggerProvider<?>> hitsLoggerMap = new HashMap<>();

  /**
   * Constructor.
   *
   * @param configuration server configuration
   */
  public HitsLoggerCreator(NrtsearchConfig configuration) {}

  private void register(Map<String, HitsLoggerProvider<? extends HitsLogger>> hitsLoggers) {
    hitsLoggers.forEach(this::register);
  }

  private void register(String name, HitsLoggerProvider<?> hitsLoggerProvider) {
    if (hitsLoggerMap.containsKey(name)) {
      throw new IllegalArgumentException("HitsLogger " + name + " already exists");
    }
    hitsLoggerMap.put(name, hitsLoggerProvider);
  }

  /**
   * Initialize singleton instance of {@link HitsLoggerCreator}. Registers the hits logger provided
   * by {@link HitsLoggerPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new HitsLoggerCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof HitsLoggerPlugin loggerPlugin) {
        instance.register(loggerPlugin.getHitsLoggers());
      }
    }
  }

  /** Get singleton instance. */
  public static HitsLoggerCreator getInstance() {
    return instance;
  }

  /**
   * Create a {@link HitsLogger} instance given the {@link LoggingHits} message from the {@link
   * com.yelp.nrtsearch.server.grpc.SearchRequest}
   *
   * @param grpcLoggingHits definition message
   * @return the corresponding hits logger
   */
  public HitsLogger createHitsLogger(LoggingHits grpcLoggingHits) {
    HitsLoggerProvider<?> provider = hitsLoggerMap.get(grpcLoggingHits.getName());
    if (provider == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown hits logger name [%s] is specified; The available hits loggers are %s",
              grpcLoggingHits.getName(), hitsLoggerMap.keySet()));
    }
    return provider.get(StructValueTransformer.transformStruct(grpcLoggingHits.getParams()));
  }
}
