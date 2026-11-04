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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  // index names being created, by bucket; in memory only
  private final Map<String, Set<String>> reservations = new HashMap<>();

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
    try {
      save();
    } catch (RuntimeException e) {
      state.buckets.remove(name);
      throw e;
    }
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
    Set<String> reserved = reservations.get(name);
    if (!bucket.indexes.isEmpty() || (reserved != null && !reserved.isEmpty())) {
      throw VectorsException.conflict(
          "Vector bucket " + name + " is not empty, delete its indexes first");
    }
    state.buckets.remove(name);
    reservations.remove(name);
    save();
  }

  /**
   * Reserve an index name in a bucket before creating the backing nrtsearch index. The reservation
   * counts toward the bucket's index limit and blocks other creates of the same name. Follow with
   * {@link #addIndex} on success or {@link #releaseIndex} on failure.
   *
   * @throws VectorsException if the bucket doesn't exist, the name is taken or reserved, or the
   *     bucket is full
   */
  public synchronized void reserveIndex(String bucketName, String indexName) {
    Bucket bucket = getBucket(bucketName);
    Set<String> reserved = reservations.computeIfAbsent(bucketName, k -> new HashSet<>());
    if (bucket.indexes.containsKey(indexName) || reserved.contains(indexName)) {
      throw VectorsException.conflict("Vector index already exists: " + indexName);
    }
    if (bucket.indexes.size() + reserved.size() >= MAX_INDEXES_PER_BUCKET) {
      throw VectorsException.validation(
          "A vector bucket can hold at most " + MAX_INDEXES_PER_BUCKET + " indexes");
    }
    reserved.add(indexName);
  }

  /** Release a reservation from {@link #reserveIndex} after a failed create. */
  public synchronized void releaseIndex(String bucketName, String indexName) {
    Set<String> reserved = reservations.get(bucketName);
    if (reserved != null) {
      reserved.remove(indexName);
    }
  }

  /** Add an index whose name was reserved with {@link #reserveIndex}. */
  public synchronized void addIndex(IndexConfig config) {
    Set<String> reserved = reservations.get(config.getBucketName());
    if (reserved == null || !reserved.remove(config.getIndexName())) {
      throw new IllegalStateException("Vector index was not reserved: " + config.getIndexName());
    }
    Bucket bucket = getBucket(config.getBucketName());
    bucket.indexes.put(config.getIndexName(), config);
    try {
      save();
    } catch (RuntimeException e) {
      // keep memory consistent with the saved registry, the caller cleans up nrtsearch
      bucket.indexes.remove(config.getIndexName());
      throw e;
    }
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
