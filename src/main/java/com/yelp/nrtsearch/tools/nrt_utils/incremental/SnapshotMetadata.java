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
package com.yelp.nrtsearch.tools.nrt_utils.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Jackson annotated class that contains metadata for an index snapshot. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class SnapshotMetadata {
  private final String serviceName;
  private final String indexName;
  private final long timestampMs;
  private final long indexSizeBytes;

  /**
   * Constructor.
   *
   * @param serviceName nrtsearch cluster service name
   * @param indexName index resource name (index-UUID)
   * @param timestampMs snapshot timestamp
   * @param indexSizeBytes size of snapshot index data in bytes
   */
  @JsonCreator
  public SnapshotMetadata(
      @JsonProperty("serviceName") String serviceName,
      @JsonProperty("indexName") String indexName,
      @JsonProperty("timestampMs") long timestampMs,
      @JsonProperty("indexSizeBytes") long indexSizeBytes) {
    this.serviceName = serviceName;
    this.indexName = indexName;
    this.timestampMs = timestampMs;
    this.indexSizeBytes = indexSizeBytes;
  }

  /** Get nrtsearch cluster service name. */
  public String getServiceName() {
    return serviceName;
  }

  /** Get index resource name (index-UUID). */
  public String getIndexName() {
    return indexName;
  }

  /** Get snapshot timestamp. */
  public long getTimestampMs() {
    return timestampMs;
  }

  /** Get size of snapshot index data in bytes. */
  public long getIndexSizeBytes() {
    return indexSizeBytes;
  }
}
