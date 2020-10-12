package com.yelp.nrtsearch.server.utils;

public class ResourceObject {

  private final String name;
  private final long creationTimestampSec;

  public ResourceObject(String name, long creationTimestampSec) {
    this.name = name;
    this.creationTimestampSec = creationTimestampSec;
  }

  public String getName() {
    return name;
  }

  public long getCreationTimestampSec() {
    return creationTimestampSec;
  }
}
