package com.yelp.nrtsearch.server.utils;

public class ResourceObjectBuilder {

  private String name;
  private long creationTimestampSec;

  public ResourceObjectBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public ResourceObjectBuilder setCreationTimestampSec(long creationTimestampSec) {
    this.creationTimestampSec = creationTimestampSec;
    return this;
  }

  public ResourceObject createResourceObject() {
    return new ResourceObject(name, creationTimestampSec);
  }
}