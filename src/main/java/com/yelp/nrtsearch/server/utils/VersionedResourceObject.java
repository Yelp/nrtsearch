package com.yelp.nrtsearch.server.utils;

import java.util.Date;

public class VersionedResourceObject {

  private final String serviceName;
  private final String resourceName;
  private final String versionHash;
  private final Date creationTimestamp;

  public VersionedResourceObject(String serviceName, String resourceName,
      String versionHash, Date creationTimestamp) {
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

