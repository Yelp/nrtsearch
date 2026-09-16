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
package com.yelp.nrtsearch.plugins.onnx;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.HashMap;
import java.util.Map;

/** Plugin that provides ONNX-based embedding providers. */
public class OnnxEmbeddingPlugin extends Plugin implements EmbeddingPlugin {
  static final String TYPE_NAME = "onnx";

  private final Map<String, EmbeddingProvider> providers = new HashMap<>();

  /** Create a new ONNX embedding plugin, initializing providers from server configuration. */
  public OnnxEmbeddingPlugin(NrtsearchConfig configuration) {
    Map<String, Map<String, Object>> configs = configuration.getEmbeddingProviderConfigs();
    for (Map.Entry<String, Map<String, Object>> entry : configs.entrySet()) {
      String type = (String) entry.getValue().get("type");
      if (TYPE_NAME.equals(type)) {
        providers.put(entry.getKey(), OnnxEmbeddingProviderFactory.create(entry.getValue()));
      }
    }
  }

  @Override
  public Map<String, EmbeddingProvider> getEmbeddingProviders() {
    return providers;
  }
}
