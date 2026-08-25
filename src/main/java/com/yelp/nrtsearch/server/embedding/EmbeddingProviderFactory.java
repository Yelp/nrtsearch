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
package com.yelp.nrtsearch.server.embedding;

import java.util.Map;

/** Factory for creating {@link EmbeddingProvider} instances from configuration. */
public interface EmbeddingProviderFactory {
  /**
   * Create a named provider instance from config.
   *
   * @param config the provider-specific config map from server YAML
   * @return configured embedding provider
   */
  EmbeddingProvider create(Map<String, Object> config);
}
