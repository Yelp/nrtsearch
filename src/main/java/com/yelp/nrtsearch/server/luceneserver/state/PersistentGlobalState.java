/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Json serializable class containing all global state that should persist between server restarts.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class PersistentGlobalState {
  private final Map<String, IndexInfo> indices;

  /** Constructor to create a new persistent state instance with default values. */
  public PersistentGlobalState() {
    this(new HashMap<>());
  }

  /**
   * Constructor.
   *
   * @param indices global index state for all known indices
   */
  @JsonCreator
  public PersistentGlobalState(@JsonProperty("indices") Map<String, IndexInfo> indices) {
    Objects.requireNonNull(indices);
    this.indices = Collections.unmodifiableMap(indices);
  }

  /** Get global index state for all known indices. Both the map and state are immutable. */
  public Map<String, IndexInfo> getIndices() {
    return indices;
  }

  /**
   * Get a builder initialized with the current state of this class. Useful when modifying only
   * specific fields.
   *
   * @return class builder
   */
  public Builder asBuilder() {
    return new Builder(this);
  }

  /** Builder for producing an immutable {@link PersistentGlobalState}. */
  public static class Builder {
    private Map<String, IndexInfo> indices;

    Builder(PersistentGlobalState base) {
      this.indices = base.indices;
    }

    public Builder withIndices(Map<String, IndexInfo> indices) {
      this.indices = indices;
      return this;
    }

    public PersistentGlobalState build() {
      return new PersistentGlobalState(this.indices);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PersistentGlobalState) {
      PersistentGlobalState p = (PersistentGlobalState) o;
      return Objects.equals(indices, p.indices);
    } else {
      return false;
    }
  }

  /**
   * Json serializable class containing index specific global state that should persist between
   * server restarts. This may eventually contain properties such as if the index is open or closed,
   * or primary discovery configuration.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(Include.NON_NULL)
  public static class IndexInfo {
    private final String id;

    /**
     * Constructor
     *
     * @param id index instance id generated at create time
     */
    @JsonCreator
    public IndexInfo(@JsonProperty("id") String id) {
      this.id = id;
    }

    /** Get index instance id (UUID). */
    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IndexInfo) {
        IndexInfo other = (IndexInfo) o;
        return Objects.equals(other.id, this.id);
      } else {
        return false;
      }
    }
  }
}
