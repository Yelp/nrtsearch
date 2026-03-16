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

import com.yelp.nrtsearch.server.grpc.Blender;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Context for a multi-retriever search. Held on {@link
 * com.yelp.nrtsearch.server.search.SearchContext} when the request contains a {@code
 * multi_retriever} field. Retriever contexts are stored in a {@link LinkedHashMap} keyed by
 * retriever name, preserving declaration order for iteration while allowing O(1) name lookup.
 */
public class MultiRetrieverContext {
  private final Map<String, RetrieverContext> retrieverContextsByName;
  private final Blender blender;

  private MultiRetrieverContext(Builder builder) {
    this.retrieverContextsByName = Collections.unmodifiableMap(builder.retrieverContextsByName);
    this.blender = builder.blender;
  }

  /**
   * Get all retriever contexts keyed by name, in declaration order. The returned map is
   * unmodifiable.
   */
  public Map<String, RetrieverContext> getRetrieverContexts() {
    return retrieverContextsByName;
  }

  /**
   * Look up a retriever context by name.
   *
   * @return the context, or {@code null} if no retriever with that name exists
   */
  public RetrieverContext getRetrieverContext(String name) {
    return retrieverContextsByName.get(name);
  }

  /** Get the blender proto definition. */
  public Blender getBlender() {
    return blender;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final LinkedHashMap<String, RetrieverContext> retrieverContextsByName =
        new LinkedHashMap<>();
    private Blender blender;

    private Builder() {}

    public Builder addRetrieverContext(RetrieverContext retrieverContext) {
      retrieverContextsByName.put(retrieverContext.getName(), retrieverContext);
      return this;
    }

    public Builder addRetrieverContexts(List<RetrieverContext> retrieverContexts) {
      retrieverContexts.forEach(ctx -> retrieverContextsByName.put(ctx.getName(), ctx));
      return this;
    }

    public Builder setBlender(Blender blender) {
      this.blender = blender;
      return this;
    }

    public MultiRetrieverContext build() {
      if (retrieverContextsByName.isEmpty()) {
        throw new IllegalStateException("retrieverContexts must be non-empty");
      }
      if (blender == null) {
        throw new IllegalStateException("blender must be set");
      }
      return new MultiRetrieverContext(this);
    }
  }
}
