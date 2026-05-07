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
package com.yelp.nrtsearch.server.search.multiretriever;

import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.search.multiretriever.blender.BlenderOperation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultiRetrieverContext {
  private final Map<String, RetrieverContext> retrieverContextMap;
  private final BlenderOperation blenderOperation;
  private final Blender blender;

  private MultiRetrieverContext(Builder builder) {
    this.retrieverContextMap = Collections.unmodifiableMap(builder.retrieverContextMap);
    this.blenderOperation = builder.blenderOperation;
    this.blender = builder.blender;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final LinkedHashMap<String, RetrieverContext> retrieverContextMap =
        new LinkedHashMap<>();
    private BlenderOperation blenderOperation;
    private Blender blender;

    private Builder() {}

    public Builder addRetrieverContext(RetrieverContext retrieverContext) {
      if (retrieverContextMap.containsKey(retrieverContext.getName())) {
        throw new IllegalArgumentException(
            "Retriever key: " + retrieverContext.getName() + " is already present");
      }
      retrieverContextMap.put(retrieverContext.getName(), retrieverContext);
      return this;
    }

    public Builder blenderOperation(BlenderOperation blenderOperation) {
      this.blenderOperation = blenderOperation;
      return this;
    }

    public Builder blender(Blender blender) {
      this.blender = blender;
      return this;
    }

    public MultiRetrieverContext build() {
      if (this.retrieverContextMap.isEmpty()) {
        throw new IllegalArgumentException(
            "RetrieverContextMap cannot be empty for MultiRetriever");
      }
      return new MultiRetrieverContext(this);
    }
  }

  public RetrieverContext getRetrieverContext(String name) {
    return this.retrieverContextMap.get(name);
  }

  public Map<String, RetrieverContext> getRetrieverContextMap() {
    return retrieverContextMap;
  }

  public BlenderOperation getBlenderOperation() {
    return blenderOperation;
  }

  public Blender getBlender() {
    return blender;
  }
}
