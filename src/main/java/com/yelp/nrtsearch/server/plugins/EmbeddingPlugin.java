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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.embedding.EmbeddingProviderFactory;
import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for providing custom {@link
 * com.yelp.nrtsearch.server.embedding.EmbeddingProvider}s. Provides info for registration of
 * embedding provider types by name.
 */
public interface EmbeddingPlugin {
  /** Get map of provider type name to {@link EmbeddingProviderFactory} for registration. */
  default Map<String, EmbeddingProviderFactory> getEmbeddingProviders() {
    return Collections.emptyMap();
  }
}
