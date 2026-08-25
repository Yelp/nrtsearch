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

import com.yelp.nrtsearch.server.embedding.EmbeddingProvider;
import com.yelp.nrtsearch.server.embedding.EmbeddingProviderFactory;
import java.util.Map;

/** Factory that creates {@link OnnxEmbeddingProvider} instances from configuration maps. */
public class OnnxEmbeddingProviderFactory implements EmbeddingProviderFactory {
  private static final int DEFAULT_MAX_TOKEN_LENGTH = 256;

  @Override
  public EmbeddingProvider create(Map<String, Object> config) {
    String modelPath = requireString(config, "modelPath");
    String tokenizerPath = requireString(config, "tokenizerPath");
    int dimensions = requireInt(config, "dimensions");
    int maxTokenLength = getInt(config, "maxTokenLength", DEFAULT_MAX_TOKEN_LENGTH);
    return new OnnxEmbeddingProvider(modelPath, tokenizerPath, dimensions, maxTokenLength);
  }

  private static String requireString(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required config key: " + key);
    }
    return value.toString();
  }

  private static int requireInt(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required config key: " + key);
    }
    return ((Number) value).intValue();
  }

  private static int getInt(Map<String, Object> config, String key, int defaultValue) {
    Object value = config.get(key);
    return value == null ? defaultValue : ((Number) value).intValue();
  }
}
