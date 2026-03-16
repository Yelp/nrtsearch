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
package com.yelp.nrtsearch.server.search.retriever;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.PluginBlender;
import com.yelp.nrtsearch.server.plugins.BlenderPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to handle the creation of {@link BlenderOperation} instances. Type strings are mapped to
 * {@link BlenderProvider}s to produce concrete {@link BlenderOperation}s.
 */
public class BlenderCreator {

  public static final String FLAT_MAP_BLENDER = "flat_map";

  private static BlenderCreator instance;

  private final Map<String, BlenderProvider<? extends BlenderOperation>> blendersMap =
      new HashMap<>();

  public BlenderCreator(NrtsearchConfig configuration) {
    register(FLAT_MAP_BLENDER, params -> new FlatMapBlenderOperation());
  }

  /**
   * Get a {@link BlenderOperation} for the given {@link Blender} config. Dispatches on the {@code
   * blender_type} oneof: built-in types ({@code flat_map}) are looked up in the registry by their
   * canonical name; plugin blenders are looked up by the name in the {@link PluginBlender} message.
   *
   * @param blender grpc blender config
   * @return blender operation
   */
  public BlenderOperation getBlenderOperation(Blender blender) {
    return switch (blender.getBlenderTypeCase()) {
      case FLAT_MAP -> blendersMap.get(FLAT_MAP_BLENDER).get(Map.of());
      case PLUGIN -> createBlender(blender.getPlugin());
      default ->
          throw new IllegalArgumentException(
              "Unsupported blender type: " + blender.getBlenderTypeCase());
    };
  }

  /**
   * Get a {@link BlenderOperation} implementation by name, and with the given parameters. Valid
   * names are built-in types or any custom type registered by a {@link BlenderPlugin}.
   *
   * @param grpcPluginBlender grpc message with plugin name and params map
   * @return blender instance
   */
  public BlenderOperation createBlender(PluginBlender grpcPluginBlender) {
    BlenderProvider<?> provider = blendersMap.get(grpcPluginBlender.getName());
    if (provider == null) {
      throw new IllegalArgumentException(
          "Invalid blender name: "
              + grpcPluginBlender.getName()
              + ", must be one of: "
              + blendersMap.keySet());
    }
    return provider.get(StructValueTransformer.transformStruct(grpcPluginBlender.getParams()));
  }

  private void register(Map<String, BlenderProvider<? extends BlenderOperation>> blenders) {
    blenders.forEach(this::register);
  }

  private void register(String name, BlenderProvider<? extends BlenderOperation> blender) {
    if (blendersMap.containsKey(name)) {
      throw new IllegalArgumentException("Blender " + name + " already exists");
    }
    blendersMap.put(name, blender);
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
