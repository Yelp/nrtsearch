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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stores vector buckets and the config of their indexes. The state is held in memory and, when a
 * file path is given, saved to a JSON file after every change. A production version would keep this
 * in S3 next to the nrtsearch global state.
 */
public class BucketRegistry {
  public static final int MAX_INDEXES_PER_BUCKET = 10_000;
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  /** A vector bucket and its indexes. */
  public static class Bucket {
    public String name;
    public long creationTime;
    public Map<String, IndexConfig> indexes = new TreeMap<>();

    public Bucket() {}

    Bucket(String name, long creationTime) {
      this.name = name;
      this.creationTime = creationTime;
    }
  }

  /** Saved registry state. */
  public static class State {
    public Map<String, Bucket> buckets = new TreeMap<>();
  }

  private final Path file;
  private State state;

  /**
   * @param file file to save the registry to, or null to keep it only in memory
   */
  public BucketRegistry(Path file) {
    this.file = file;
    this.state = load(file);
  }

  private static State load(Path file) {
    if (file == null || !Files.exists(file)) {
      return new State();
    }
    try {
      return MAPPER.readValue(file.toFile(), State.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read bucket registry " + file, e);
    }
  }

  /** Save the registry. Called after every change. */
  public synchronized void save() {
    if (file == null) {
      return;
    }
    try {
      Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
      MAPPER.writeValue(tmp.toFile(), state);
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not save bucket registry " + file, e);
    }
  }

  public synchronized Bucket createBucket(String name) {
    IndexConfig.validateName("Vector bucket", name);
    if (state.buckets.containsKey(name)) {
      throw VectorsException.conflict("Vector bucket already exists: " + name);
    }
    Bucket bucket = new Bucket(name, System.currentTimeMillis());
    state.buckets.put(name, bucket);
    save();
    return bucket;
  }

  public synchronized Bucket getBucket(String name) {
    Bucket bucket = state.buckets.get(name);
    if (bucket == null) {
      throw VectorsException.notFound("Vector bucket not found: " + name);
    }
    return bucket;
  }

  public synchronized List<Bucket> listBuckets() {
    return new ArrayList<>(state.buckets.values());
  }

  public synchronized void deleteBucket(String name) {
    Bucket bucket = getBucket(name);
    if (!bucket.indexes.isEmpty()) {
      throw VectorsException.conflict(
          "Vector bucket " + name + " is not empty, delete its indexes first");
    }
    state.buckets.remove(name);
    save();
  }

  public synchronized void addIndex(IndexConfig config) {
    Bucket bucket = getBucket(config.getBucketName());
    if (bucket.indexes.containsKey(config.getIndexName())) {
      throw VectorsException.conflict("Vector index already exists: " + config.getIndexName());
    }
    if (bucket.indexes.size() >= MAX_INDEXES_PER_BUCKET) {
      throw VectorsException.validation(
          "A vector bucket can hold at most " + MAX_INDEXES_PER_BUCKET + " indexes");
    }
    bucket.indexes.put(config.getIndexName(), config);
    save();
  }

  public synchronized IndexConfig getIndex(String bucketName, String indexName) {
    IndexConfig config = getBucket(bucketName).indexes.get(indexName);
    if (config == null) {
      throw VectorsException.notFound(
          "Vector index not found: " + indexName + " in bucket " + bucketName);
    }
    return config;
  }

  public synchronized List<IndexConfig> listIndexes(String bucketName) {
    return new ArrayList<>(getBucket(bucketName).indexes.values());
  }

  public synchronized List<IndexConfig> allIndexes() {
    List<IndexConfig> all = new ArrayList<>();
    state.buckets.values().forEach(b -> all.addAll(b.indexes.values()));
    return all;
  }

  public synchronized void removeIndex(String bucketName, String indexName) {
    getIndex(bucketName, indexName);
    getBucket(bucketName).indexes.remove(indexName);
    save();
  }
}
