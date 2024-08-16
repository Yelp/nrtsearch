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
package com.yelp.nrtsearch.server.luceneserver.logging;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;

/** Factory class that handles registration and creation of {@link HitsLogger}s. */
public class HitsLoggerCreator {
  private static HitsLoggerCreator instance;
  private HitsLoggerProvider<?> hitsLoggerProvider;

  /**
   * Constructor.
   *
   * @param configuration server configuration
   */
  public HitsLoggerCreator(LuceneServerConfiguration configuration) {}

  private void register(HitsLoggerProvider<?> hitsLogger) {
    if (this.hitsLoggerProvider != null) {
      throw new IllegalArgumentException("Hits logger already exists");
    }
    this.hitsLoggerProvider = hitsLogger;
  }

  /**
   * Initialize singleton instance of {@link HitsLoggerCreator}. Registers the hits logger provided
   * by {@link HitsLoggerPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new HitsLoggerCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof HitsLoggerPlugin loggerPlugin) {
        instance.register(loggerPlugin.getHitsLogger());
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
    HitsLoggerProvider<?> provider = this.hitsLoggerProvider;
    if (this.hitsLoggerProvider == null) {
      throw new IllegalArgumentException("No hits logger was assigned");
    }
    return provider.get(StructValueTransformer.transformStruct(grpcLoggingHits.getParams()));
  }
}
