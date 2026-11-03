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
package com.yelp.nrtsearch.vectors;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration for one vector index (S3 Vectors CreateIndex input), plus the mutable metadata
 * schema discovered from PutVectors calls.
 */
public class IndexConfig {
  public static final int MAX_DIMENSION = 4096;
  public static final int MAX_NON_FILTERABLE_KEYS = 10;
  public static final int MAX_KEY_NAME_LENGTH = 63;
  private static final Pattern NAME_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$");

  /** S3 Vectors distance metrics. */
  public enum DistanceMetric {
    COSINE("cosine"),
    EUCLIDEAN("l2_norm");

    private final String nrtsearchSimilarity;

    DistanceMetric(String nrtsearchSimilarity) {
      this.nrtsearchSimilarity = nrtsearchSimilarity;
    }

    public String getNrtsearchSimilarity() {
      return nrtsearchSimilarity;
    }

    public static DistanceMetric parse(String value) {
      if (value == null) {
        throw VectorsException.validation("distanceMetric is required");
      }
      try {
        return DistanceMetric.valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw VectorsException.validation(
            "distanceMetric must be one of [cosine, euclidean], got: " + value);
      }
    }

    public String apiName() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private String bucketName;
  private String indexName;
  private int dimension;
  private DistanceMetric distanceMetric;
  private List<String> nonFilterableMetadataKeys = new ArrayList<>();
  private long creationTime;
  private MetadataSchema metadataSchema = new MetadataSchema();

  /** For JSON deserialization. */
  public IndexConfig() {}

  public IndexConfig(
      String bucketName,
      String indexName,
      int dimension,
      DistanceMetric distanceMetric,
      List<String> nonFilterableMetadataKeys,
      long creationTime) {
    this.bucketName = bucketName;
    this.indexName = indexName;
    this.dimension = dimension;
    this.distanceMetric = distanceMetric;
    this.nonFilterableMetadataKeys =
        nonFilterableMetadataKeys == null ? new ArrayList<>() : nonFilterableMetadataKeys;
    this.creationTime = creationTime;
    validate();
  }

  public static void validateName(String kind, String name) {
    if (name == null || !NAME_PATTERN.matcher(name).matches()) {
      throw VectorsException.validation(
          kind
              + " name must be 3-63 characters of lowercase letters, numbers and hyphens, "
              + "starting and ending with a letter or number: "
              + name);
    }
  }

  private void validate() {
    validateName("Vector bucket", bucketName);
    validateName("Vector index", indexName);
    if (dimension < 1 || dimension > MAX_DIMENSION) {
      throw VectorsException.validation(
          "dimension must be between 1 and " + MAX_DIMENSION + ", got: " + dimension);
    }
    if (distanceMetric == null) {
      throw VectorsException.validation("distanceMetric is required");
    }
    if (nonFilterableMetadataKeys.size() > MAX_NON_FILTERABLE_KEYS) {
      throw VectorsException.validation(
          "At most " + MAX_NON_FILTERABLE_KEYS + " non-filterable metadata keys are allowed");
    }
    Set<String> seen = new LinkedHashSet<>();
    for (String key : nonFilterableMetadataKeys) {
      if (key == null || key.isEmpty() || key.length() > MAX_KEY_NAME_LENGTH) {
        throw VectorsException.validation(
            "Non-filterable metadata key names must be 1-" + MAX_KEY_NAME_LENGTH + " characters");
      }
      if (!seen.add(key)) {
        throw VectorsException.validation("Duplicate non-filterable metadata key: " + key);
      }
    }
  }

  /** Name of the backing nrtsearch index. */
  public String nrtsearchIndexName() {
    return nrtsearchIndexName(bucketName, indexName);
  }

  public static String nrtsearchIndexName(String bucketName, String indexName) {
    // nrtsearch index names allow [A-z0-9_-]; bucket and index names never contain '_'
    return "vb_" + bucketName + "__" + indexName;
  }

  public boolean isFilterable(String metadataKey) {
    return !nonFilterableMetadataKeys.contains(metadataKey);
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getIndexName() {
    return indexName;
  }

  public int getDimension() {
    return dimension;
  }

  public DistanceMetric getDistanceMetric() {
    return distanceMetric;
  }

  public List<String> getNonFilterableMetadataKeys() {
    return nonFilterableMetadataKeys;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public MetadataSchema getMetadataSchema() {
    return metadataSchema;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public void setDistanceMetric(DistanceMetric distanceMetric) {
    this.distanceMetric = distanceMetric;
  }

  public void setNonFilterableMetadataKeys(List<String> nonFilterableMetadataKeys) {
    this.nonFilterableMetadataKeys = nonFilterableMetadataKeys;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public void setMetadataSchema(MetadataSchema metadataSchema) {
    this.metadataSchema = metadataSchema;
  }
}
