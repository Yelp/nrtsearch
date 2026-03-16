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

import com.yelp.nrtsearch.server.plugins.BlenderPlugin;
import java.util.LinkedHashMap;
import org.apache.lucene.search.TopDocs;

/**
 * Interface for blending results from multiple retrievers into a single ranked {@link TopDocs}.
 * Implementations are registered by name via {@link BlenderPlugin} and instantiated through {@link
 * BlenderCreator}.
 *
 * <p>The input is an ordered list of {@link TopDocs} (one per retriever, in declaration order) and
 * their corresponding {@link RetrieverContext}s. The output is a single {@link TopDocs} containing
 * the blended and deduplicated results.
 */
public interface BlenderOperation {

  /**
   * Blend results from multiple retrievers into a single ranked {@link TopDocs}.
   *
   * @param retrieverResults per-retriever results in declaration order, keyed by retriever name
   * @param blender the blender proto definition (contains top_hits, start_hit, and type-specific
   *     config)
   * @param retrieverContexts per-retriever contexts in declaration order, keyed by retriever name
   * @return blended results as a single {@link TopDocs}
   */
  TopDocs blend(
      LinkedHashMap<String, TopDocs> retrieverResults,
      com.yelp.nrtsearch.server.grpc.Blender blender,
      LinkedHashMap<String, RetrieverContext> retrieverContexts);
}
