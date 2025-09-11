/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.config;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import java.util.HashMap;
import java.util.Map;

public class IngestionPluginConfigs {

  private final Map<String, Map<String, Object>> pluginConfigs = new HashMap<>();

  @JsonAnySetter
  public void addPluginConfig(String pluginName, Map<String, Object> config) {
    pluginConfigs.put(pluginName, config);
  }

  public Map<String, Map<String, Object>> getPluginConfigs() {
    return pluginConfigs;
  }
}
