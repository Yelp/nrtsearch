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

/** Configuration for various ThreadPool Settings used in nrtsearch */
public class ThreadPoolConfiguration {

  private static final int DEFAULT_MAX_SEARCHING_THREADS =
      ((Runtime.getRuntime().availableProcessors() * 3) / 2) + 1;
  private static final int DEFAULT_MAX_SEARCH_BUFFERED_ITEMS =
      Math.max(1000, 2 * DEFAULT_MAX_SEARCHING_THREADS);

  private static final int DEFAULT_MAX_INDEXING_THREADS =
      Runtime.getRuntime().availableProcessors() + 1;
  private static final int DEFAULT_MAX_INDEXING_BUFFERED_ITEMS =
      Math.max(200, 2 * DEFAULT_MAX_INDEXING_THREADS);

  private static final int DEFAULT_MAX_GRPC_LUCENESERVER_THREADS = DEFAULT_MAX_INDEXING_THREADS;
  private static final int DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS =
      DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

  private static final int DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS =
      DEFAULT_MAX_INDEXING_THREADS;
  private static final int DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS =
      DEFAULT_MAX_INDEXING_BUFFERED_ITEMS;

  private final int maxSearchingThreads;
  private final int maxSearchBufferedItems;

  private final int maxIndexingThreads;
  private final int maxIndexingBufferedItems;

  private final int maxGrpcLuceneserverThreads;
  private final int maxGrpcLuceneserverBufferedItems;

  private final int maxGrpcReplicationserverThreads;
  private final int maxGrpcReplicationserverBufferedItems;

  public ThreadPoolConfiguration(YamlConfigReader configReader) {
    maxSearchingThreads =
        configReader.getInteger(
            "threadPoolConfiguration.maxSearchingThreads", DEFAULT_MAX_SEARCHING_THREADS);
    maxSearchBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxSearchBufferedItems", DEFAULT_MAX_SEARCH_BUFFERED_ITEMS);

    maxIndexingThreads =
        configReader.getInteger(
            "threadPoolConfiguration.maxIndexingThreads", DEFAULT_MAX_INDEXING_THREADS);
    maxIndexingBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxIndexingBufferedItems",
            DEFAULT_MAX_INDEXING_BUFFERED_ITEMS);

    maxGrpcLuceneserverThreads =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcLuceneserverThreads",
            DEFAULT_MAX_GRPC_LUCENESERVER_THREADS);
    maxGrpcLuceneserverBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcLuceneserverBufferedItems",
            DEFAULT_MAX_GRPC_LUCENESERVER_BUFFERED_ITEMS);

    maxGrpcReplicationserverThreads =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcReplicationserverThreads",
            DEFAULT_MAX_GRPC_REPLICATIONSERVER_THREADS);
    maxGrpcReplicationserverBufferedItems =
        configReader.getInteger(
            "threadPoolConfiguration.maxGrpcReplicationserverBufferedItems",
            DEFAULT_MAX_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS);
  }

  public int getMaxSearchingThreads() {
    return maxSearchingThreads;
  }

  public int getMaxSearchBufferedItems() {
    return maxSearchBufferedItems;
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
