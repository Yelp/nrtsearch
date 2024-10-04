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

import com.yelp.nrtsearch.server.similarity.SimilarityProvider;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Plugin interface for providing custom {@link Similarity} implementations. The registered types
 * can be used by specifying the type name in the {@link com.yelp.nrtsearch.server.grpc.Field}
 * definition message. Similarity parameters can be provided through the similarityParams field.
 */
public interface SimilarityPlugin {

  /**
   * Provide a map of custom {@link Similarity} implementations to register. The implementations can
   * be used by specifying the type in the {@link com.yelp.nrtsearch.server.grpc.Field} definition.
   *
   * @return map of similarities to register
   */
  default Map<String, SimilarityProvider<? extends Similarity>> getSimilarities() {
    return Collections.emptyMap();
  }
}
