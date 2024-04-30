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
package com.yelp.nrtsearch.server.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.util.Map;

/** Configuration for various ThreadPool Settings used in nrtsearch */
public class ThreadPoolConfiguration {

  private static final int DEFAULT_MAX_SEARCHING_THREADS =
      ((Runtime.getRuntime().availableProcessors() * 3) / 2) + 1;
  private static final int DEFAULT_MAX_SEARCH_BUFFERED_ITEMS =
      Math.max(1000, 2 * DEFAULT_MAX_SEARCHING_THREADS);

  private static final int DEFAULT_MAX_INDEXING_THREADS =
      Runtime.getRuntime().availableProcessors() + 1;
  private static final int DEFAULT_MAX_FILL_FIELDS_THREADS = 1;

  private static final int DEFAULT_MAX_INDEXING_BUFFERED_ITEMS =
      Math.max(200, 2 * DEFAULT_MAX_INDEXING_THREADS);

  private static final int DEFAULT_MAX_GRPC_LUCENESERVER_THREADS = DEFAULT_MAX_INDEXING_THREADS;
  private static final int DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS =
      DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

  private static final int DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS =
      DEFAULT_MAX_INDEXING_THREADS;
  private static final int DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS =
      DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

  public static final int DEFAULT_MIN_PARALLEL_FETCH_NUM_FIELDS = 20;
  public static final int DEFAULT_MIN_PARALLEL_FETCH_NUM_HITS = 50;

  private final int maxSearchingThreads;
  private final int maxSearchBufferedItems;

  private final int maxFetchThreads;
  private final int minParallelFetchNumFields;
  private final int minParallelFetchNumHits;
  private final boolean parallelFetchByField;

  private final int maxIndexingThreads;
  private final int maxIndexingBufferedItems;

  private final int maxGrpcLuceneserverThreads;
  private final int maxGrpcLuceneserverBufferedItems;

  private final int maxGrpcReplicationserverThreads;
  private final int maxGrpcReplicationserverBufferedItems;

  public ThreadPoolConfiguration(YamlConfigReader configReader) {
    maxSearchingThreads =
        getNumThreads(
            configReader,
            "threadPoolConfiguration.maxSearchingThreads",
            DEFAULT_MAX_SEARCHING_THREADS);
    maxSearchBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxSearchBufferedItems", DEFAULT_MAX_SEARCH_BUFFERED_ITEMS);
    maxFetchThreads =
        getNumThreads(
            configReader,
            "threadPoolConfiguration.maxFetchThreads",
            DEFAULT_MAX_FILL_FIELDS_THREADS);
    minParallelFetchNumFields =
        configReader.getInteger(
            "threadPoolConfiguration.minParallelFetchNumFields",
            DEFAULT_MIN_PARALLEL_FETCH_NUM_FIELDS);
    minParallelFetchNumHits =
        configReader.getInteger(
            "threadPoolConfiguration.minParallelFetchNumHits", DEFAULT_MIN_PARALLEL_FETCH_NUM_HITS);
    parallelFetchByField =
        configReader.getBoolean("threadPoolConfiguration.parallelFetchByField", true);

    maxIndexingThreads =
        getNumThreads(
            configReader,
            "threadPoolConfiguration.maxIndexingThreads",
            DEFAULT_MAX_INDEXING_THREADS);
    maxIndexingBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxIndexingBufferedItems",
            DEFAULT_MAX_INDEXING_BUFFERED_ITEMS);

    maxGrpcLuceneserverThreads =
        getNumThreads(
            configReader,
            "threadPoolConfiguration.maxGrpcLuceneserverThreads",
            DEFAULT_MAX_GRPC_LUCENESERVER_THREADS);
    maxGrpcLuceneserverBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcLuceneserverBufferedItems",
            DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS);

    maxGrpcReplicationserverThreads =
        getNumThreads(
            configReader,
            "threadPoolConfiguration.maxGrpcReplicationserverThreads",
            DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS);
    maxGrpcReplicationserverBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcReplicationserverBufferedItems",
            DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class ThreadsConfig {
    private int min = 1;
    private int max = Integer.MAX_VALUE;
    private int offset = 0;
    private float multiplier = 1.0f;

    public void setMin(int min) {
      if (min <= 0) {
        throw new IllegalArgumentException("min must be >= 1");
      }
      this.min = min;
    }

    public void setMax(int max) {
      if (max <= 0) {
        throw new IllegalArgumentException("max must be >= 1");
      }
      this.max = max;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public void setMultiplier(float multiplier) {
      this.multiplier = multiplier;
    }

    public int computeNumThreads() {
      int threads = (int) ((Runtime.getRuntime().availableProcessors() * multiplier) + offset);
      threads = Math.min(threads, max);
      threads = Math.max(threads, min);
      return threads;
    }
  }

  static int getNumThreads(YamlConfigReader configReader, String key, int defaultValue) {
    return configReader.get(
        key,
        obj -> {
          if (obj instanceof Number) {
            return ((Number) obj).intValue();
          } else if (obj instanceof Map) {
            return JsonUtils.convertValue(obj, ThreadsConfig.class).computeNumThreads();
          } else {
            throw new IllegalArgumentException(
                "Invalid thread pool config type: " + obj.getClass() + ", key: " + key);
          }
        },
        defaultValue);
  }

  public int getMaxSearchingThreads() {
    return maxSearchingThreads;
  }

  public int getMaxSearchBufferedItems() {
    return maxSearchBufferedItems;
  }

  public int getMaxFetchThreads() {
    return maxFetchThreads;
  }

  public int getMinParallelFetchNumFields() {
    return minParallelFetchNumFields;
  }

  public int getMinParallelFetchNumHits() {
    return minParallelFetchNumHits;
  }

  public boolean getParallelFetchByField() {
    return parallelFetchByField;
  }

  public int getMaxIndexingThreads() {
    return maxIndexingThreads;
  }

  public int getMaxIndexingBufferedItems() {
    return maxIndexingBufferedItems;
  }

  public int getMaxGrpcLuceneserverThreads() {
    return maxGrpcLuceneserverThreads;
  }

  public int getMaxGrpcReplicationserverThreads() {
    return maxGrpcReplicationserverThreads;
  }

  public int getMaxGrpcLuceneserverBufferedItems() {
    return maxGrpcLuceneserverBufferedItems;
  }

  public int getMaxGrpcReplicationserverBufferedItems() {
    return maxGrpcReplicationserverBufferedItems;
  }
}
