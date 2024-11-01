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
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.utils.JsonUtils;
import java.util.HashMap;
import java.util.Map;

/** Configuration for various ThreadPool Settings used in nrtsearch */
public class ThreadPoolConfiguration {
  public static final String CONFIG_PREFIX = "threadPoolConfiguration.";

  private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
  public static final int DEFAULT_SEARCHING_THREADS = ((AVAILABLE_PROCESSORS * 3) / 2) + 1;
  public static final int DEFAULT_SEARCH_BUFFERED_ITEMS =
      Math.max(1000, 2 * DEFAULT_SEARCHING_THREADS);

  public static final int DEFAULT_INDEXING_THREADS = AVAILABLE_PROCESSORS + 1;
  public static final int DEFAULT_INDEXING_BUFFERED_ITEMS =
      Math.max(200, 2 * DEFAULT_INDEXING_THREADS);

  public static final int DEFAULT_GRPC_SERVER_THREADS = DEFAULT_INDEXING_THREADS;
  public static final int DEFAULT_GRPC_SERVER_BUFFERED_ITEMS = DEFAULT_INDEXING_BUFFERED_ITEMS;

  public static final int DEFAULT_GRPC_REPLICATIONSERVER_THREADS = DEFAULT_INDEXING_THREADS;
  public static final int DEFAULT_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS =
      DEFAULT_INDEXING_BUFFERED_ITEMS;

  public static final int DEFAULT_FETCH_THREADS = 1;
  public static final int DEFAULT_FETCH_BUFFERED_ITEMS = DEFAULT_SEARCH_BUFFERED_ITEMS;

  public static final int DEFAULT_GRPC_THREADS = AVAILABLE_PROCESSORS * 2;
  public static final int DEFAULT_GRPC_BUFFERED_ITEMS = 8;

  public static final int DEFAULT_METRICS_THREADS = AVAILABLE_PROCESSORS;
  public static final int DEFAULT_METRICS_BUFFERED_ITEMS = 8;

  public static final int DEFAULT_VECTOR_MERGE_THREADS = AVAILABLE_PROCESSORS;
  public static final int DEFAULT_VECTOR_MERGE_BUFFERED_ITEMS =
      Math.max(100, 2 * DEFAULT_VECTOR_MERGE_THREADS);

  /**
   * Settings for a {@link ExecutorFactory.ExecutorType}.
   *
   * @param maxThreads max number of threads
   * @param maxBufferedItems max number of buffered items
   * @param threadNamePrefix prefix for thread names
   */
  public record ThreadPoolSettings(int maxThreads, int maxBufferedItems, String threadNamePrefix) {}

  private static final Map<ExecutorFactory.ExecutorType, ThreadPoolSettings>
      defaultThreadPoolSettings =
          Map.of(
              ExecutorFactory.ExecutorType.SEARCH,
              new ThreadPoolSettings(
                  DEFAULT_SEARCHING_THREADS, DEFAULT_SEARCH_BUFFERED_ITEMS, "LuceneSearchExecutor"),
              ExecutorFactory.ExecutorType.INDEX,
              new ThreadPoolSettings(
                  DEFAULT_INDEXING_THREADS,
                  DEFAULT_INDEXING_BUFFERED_ITEMS,
                  "LuceneIndexingExecutor"),
              ExecutorFactory.ExecutorType.SERVER,
              new ThreadPoolSettings(
                  DEFAULT_GRPC_SERVER_THREADS,
                  DEFAULT_GRPC_SERVER_BUFFERED_ITEMS,
                  "GrpcServerExecutor"),
              ExecutorFactory.ExecutorType.REPLICATIONSERVER,
              new ThreadPoolSettings(
                  DEFAULT_GRPC_REPLICATIONSERVER_THREADS,
                  DEFAULT_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS,
                  "GrpcReplicationServerExecutor"),
              ExecutorFactory.ExecutorType.FETCH,
              new ThreadPoolSettings(
                  DEFAULT_FETCH_THREADS, DEFAULT_FETCH_BUFFERED_ITEMS, "LuceneFetchExecutor"),
              ExecutorFactory.ExecutorType.GRPC,
              new ThreadPoolSettings(
                  DEFAULT_GRPC_THREADS, DEFAULT_GRPC_BUFFERED_ITEMS, "GrpcExecutor"),
              ExecutorFactory.ExecutorType.METRICS,
              new ThreadPoolSettings(
                  DEFAULT_METRICS_THREADS, DEFAULT_METRICS_BUFFERED_ITEMS, "MetricsExecutor"),
              ExecutorFactory.ExecutorType.VECTORMERGE,
              new ThreadPoolSettings(
                  DEFAULT_VECTOR_MERGE_THREADS,
                  DEFAULT_VECTOR_MERGE_BUFFERED_ITEMS,
                  "VectorMergeExecutor"));

  private final Map<ExecutorFactory.ExecutorType, ThreadPoolSettings> threadPoolSettings;

  public ThreadPoolConfiguration(YamlConfigReader configReader) {
    threadPoolSettings = new HashMap<>();
    for (ExecutorFactory.ExecutorType executorType : ExecutorFactory.ExecutorType.values()) {
      ThreadPoolSettings defaultSettings = defaultThreadPoolSettings.get(executorType);
      String poolConfigPrefix = CONFIG_PREFIX + executorType.name().toLowerCase() + ".";
      int maxThreads =
          getNumThreads(
              configReader, poolConfigPrefix + "maxThreads", defaultSettings.maxThreads());
      int maxBufferedItems =
          configReader.getInteger(
              poolConfigPrefix + "maxBufferedItems", defaultSettings.maxBufferedItems());
      String threadNamePrefix =
          configReader.getString(
              poolConfigPrefix + "threadNamePrefix", defaultSettings.threadNamePrefix());
      threadPoolSettings.put(
          executorType, new ThreadPoolSettings(maxThreads, maxBufferedItems, threadNamePrefix));
    }
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
      int threads = (int) ((AVAILABLE_PROCESSORS * multiplier) + offset);
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

  public ThreadPoolSettings getThreadPoolSettings(ExecutorFactory.ExecutorType executorType) {
    return threadPoolSettings.get(executorType);
  }
}
