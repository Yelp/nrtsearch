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
package com.yelp.nrtsearch.server.search.multiretriever.blender;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.PluginBlender;
import com.yelp.nrtsearch.server.plugins.BlenderPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.search.multiretriever.blender.operation.ScorelessRawMergeBlenderOperation;
import com.yelp.nrtsearch.server.search.multiretriever.blender.operation.WeightedRrfBlenderOperation;
import com.yelp.nrtsearch.server.search.multiretriever.blender.operation.WeightedScoreOrderBlenderOperation;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/** Class to handle the creation of {@link BlenderOperation} instances. */
public class BlenderCreator {

  private static BlenderCreator instance;

  private final Map<String, BlenderProvider<? extends BlenderOperation>> pluginsMap =
      new HashMap<>();

  public BlenderCreator(NrtsearchConfig configuration) {}

  /**
   * Get a {@link BlenderOperation} for the given {@link Blender} config. Dispatches on the {@code
   * blender_type} oneof: built-in types are instantiated directly from the config; plugin blenders
   * are instantiated via their registered {@link BlenderProvider} with the decoded {@link
   * PluginBlender#getParams()}.
   *
   * @param blender grpc blender config
   * @return blender operation
   */
  public BlenderOperation getBlenderOperation(Blender blender) {
    return switch (blender.getBlenderTypeCase()) {
      case WEIGHTEDRRF -> new WeightedRrfBlenderOperation(blender.getWeightedRrf());
      case WEIGHTEDSCOREORDER ->
          new WeightedScoreOrderBlenderOperation(blender.getWeightedScoreOrder());
      case SCORELESSRAWMERGE -> new ScorelessRawMergeBlenderOperation();
      case PLUGIN -> getPluginBlender(blender.getPlugin());
      default ->
          throw new IllegalArgumentException(
              "Unsupported blender type: " + blender.getBlenderTypeCase());
    };
  }

  /**
   * Look up a registered plugin {@link BlenderProvider} by name and instantiate it with the decoded
   * params from the {@link PluginBlender} message.
   *
   * @param grpcPluginBlender grpc message carrying the plugin name and params
   * @return blender operation instance
   */
  public BlenderOperation getPluginBlender(PluginBlender grpcPluginBlender) {
    BlenderProvider<?> provider = pluginsMap.get(grpcPluginBlender.getName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Invalid blender name: "
              + grpcPluginBlender.getName()
              + ", must be one of: "
              + pluginsMap.keySet());
    }
    return provider.get(StructValueTransformer.transformStruct(grpcPluginBlender.getParams()));
  }

  private void register(Map<String, BlenderProvider<? extends BlenderOperation>> blenders) {
    blenders.forEach(this::register);
  }

  private void register(String name, BlenderProvider<? extends BlenderOperation> provider) {
    if (pluginsMap.containsKey(name)) {
      throw new IllegalArgumentException("Blender " + name + " already exists");
    }
    pluginsMap.put(name, provider);
  }

  /**
   * Initialize singleton instance of {@link BlenderCreator}. Registers any additional {@link
   * BlenderOperation} implementations provided by {@link BlenderPlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(NrtsearchConfig configuration, Iterable<Plugin> plugins) {
    instance = new BlenderCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof BlenderPlugin blenderPlugin) {
        instance.register(blenderPlugin.getBlenders());
      }
    }
  }

  /** Get singleton instance. */
  public static BlenderCreator getInstance() {
    return instance;
  }
}
