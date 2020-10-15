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
package com.yelp.nrtsearch.server.utils;

import java.util.Date;

public class VersionedResourceObject {

  private final String serviceName;
  private final String resourceName;
  private final String versionHash;
  private final Date creationTimestamp;

  public VersionedResourceObject(
      String serviceName, String resourceName, String versionHash, Date creationTimestamp) {
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

  public Date getCreationTimestamp() {
    return creationTimestamp;
  }

  public static VersionedResourceObjectBuilder builder() {
    return new VersionedResourceObjectBuilder();
  }

  public static class VersionedResourceObjectBuilder {

    private String serviceName;
    private String resourceName;
    private String versionHash;
    private Date creationTimestamp;

    public VersionedResourceObjectBuilder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public VersionedResourceObjectBuilder setResourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

    public VersionedResourceObjectBuilder setVersionHash(String versionHash) {
      this.versionHash = versionHash;
      return this;
    }

    public VersionedResourceObjectBuilder setCreationTimestamp(Date creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
      return this;
    }

    public VersionedResourceObject createVersionedResourceObject() {
      return new VersionedResourceObject(serviceName, resourceName, versionHash, creationTimestamp);
    }
  }
}
