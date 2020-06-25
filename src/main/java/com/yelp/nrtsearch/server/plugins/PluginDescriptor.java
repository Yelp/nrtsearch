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
package com.yelp.nrtsearch.server.plugins;

/** Class that contain all information related to a loaded plugin instance. */
public class PluginDescriptor {
  private final Plugin plugin;
  private final PluginMetadata metadata;

  PluginDescriptor(Plugin plugin, PluginMetadata metadata) {
    this.plugin = plugin;
    this.metadata = metadata;
  }

  /** Get the {@link Plugin} class instance. */
  public Plugin getPlugin() {
    return plugin;
  }

  /** Get the {@link PluginMetadata} loaded for this plugin. */
  public PluginMetadata getPluginMetadata() {
    return metadata;
  }
}
