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
package com.yelp.nrtsearch.server.rescore;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.PluginRescorer;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.RescorerPlugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to handle the creation of {@link RescoreOperation} instances. Type strings are mapped to
 * {@link RescorerProvider}s to produce concrete {@link RescoreOperation}s.
 */
public class RescorerCreator {

  private static RescorerCreator instance;

  private final Map<String, RescorerProvider<? extends RescoreOperation>> rescorersMap =
      new HashMap<>();

  public RescorerCreator(NrtsearchConfig configuration) {}

  /**
   * Get a {@link RescoreOperation} implementation by name, and with the given parameters. Valid
   * names are any custom type registered by a {@link RescorerPlugin}.
   *
   * @param grpcPluginRescorer grpc message with plugin name and params map
   * @return rescorer instance
   */
  public RescoreOperation createRescorer(PluginRescorer grpcPluginRescorer) {
    RescorerProvider<?> provider = rescorersMap.get(grpcPluginRescorer.getName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Invalid rescorer name: "
              + grpcPluginRescorer.getName()
              + ", must be one of: "
              + rescorersMap.keySet());
    }
    return provider.get(StructValueTransformer.transformStruct(grpcPluginRescorer.getParams()));
  }

  private void register(Map<String, RescorerProvider<? extends RescoreOperation>> rescorers) {
    rescorers.forEach(this::register);
  }

  private void register(String name, RescorerProvider<? extends RescoreOperation> rescorer) {
    if (rescorersMap.containsKey(name)) {
      throw new IllegalArgumentException("Rescorer " + name + " already exists");
    }
    rescorersMap.put(name, rescorer);
  }

  /**
   * Initialize singleton instance of {@link RescorerCreator}. Registers any additional {@link
   * RescoreOperation} implementations provided by {@link RescorerPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new RescorerCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof RescorerPlugin rescorePlugin) {
        instance.register(rescorePlugin.getRescorers());
      }
    }
  }

  /** Get singleton instance. */
  public static RescorerCreator getInstance() {
    return instance;
  }
}
