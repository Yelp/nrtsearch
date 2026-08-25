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
import com.yelp.nrtsearch.server.embedding.EmbeddingProviderFactory;
import com.yelp.nrtsearch.server.plugins.EmbeddingPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.Map;

/** Plugin that registers the ONNX embedding provider type. */
public class OnnxEmbeddingPlugin extends Plugin implements EmbeddingPlugin {

  /** Create a new ONNX embedding plugin. */
  public OnnxEmbeddingPlugin(NrtsearchConfig configuration) {}

  @Override
  public Map<String, EmbeddingProviderFactory> getEmbeddingProviders() {
    return Map.of("onnx", new OnnxEmbeddingProviderFactory());
  }
}
