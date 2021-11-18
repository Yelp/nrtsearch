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
package com.yelp.nrtsearch.server.backup;

import java.time.Instant;
import java.util.Objects;

public class VersionedResource {

  private final String serviceName;
  private final String resourceName;
  private final String versionHash;
  private final Instant creationTimestamp;

  public VersionedResource(
      String serviceName, String resourceName, String versionHash, Instant creationTimestamp) {
    this.serviceName = serviceName;
    this.resourceName = resourceName;
    this.versionHash = versionHash;
    this.creationTimestamp = creationTimestamp;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getVersionHash() {
    return versionHash;
  }

  public Instant getCreationTimestamp() {
    return creationTimestamp;
  }

  @Override
  public String toString() {
    return "VersionedResource{"
        + "serviceName='"
        + serviceName
        + '\''
        + ", resourceName='"
        + resourceName
        + '\''
        + ", versionHash='"
        + versionHash
        + '\''
        + ", creationTimestamp="
        + creationTimestamp
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VersionedResource that = (VersionedResource) o;
    return Objects.equals(serviceName, that.serviceName)
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(versionHash, that.versionHash)
        && Objects.equals(creationTimestamp, that.creationTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, resourceName, versionHash, creationTimestamp);
  }

  public static VersionedResourceBuilder builder() {
    return new VersionedResourceBuilder();
  }

  public static class VersionedResourceBuilder {

    private String serviceName;
    private String resourceName;
    private String versionHash;
    private Instant creationTimestamp;

    public VersionedResourceBuilder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public VersionedResourceBuilder setResourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public VersionedResourceBuilder setVersionHash(String versionHash) {
      this.versionHash = versionHash;
      return this;
    }

    public VersionedResourceBuilder setCreationTimestamp(Instant creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
      return this;
    }

    public VersionedResource createVersionedResource() {
      return new VersionedResource(serviceName, resourceName, versionHash, creationTimestamp);
    }
  }
}
