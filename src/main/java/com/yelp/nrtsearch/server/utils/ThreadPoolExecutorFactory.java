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

import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static Factory to generate {@link ThreadPoolExecutor} as per the {@link ExecutorType} provided
 */
public class ThreadPoolExecutorFactory {
  public enum ExecutorType {
    SEARCH,
    INDEX,
    LUCENESERVER,
    REPLICATIONSERVER,
    FETCH,
    GRPC,
    METRICS
  }

  private static final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorFactory.class);
  private static final int DEFAULT_QUEUE_SIZE = 8;

  /**
   * @param executorType {@link ExecutorType}
   * @param threadPoolConfiguration {@link ThreadPoolConfiguration}
   * @return {@link ThreadPoolExecutor}
   */
  public static ThreadPoolExecutor getThreadPoolExecutor(
      ExecutorType executorType, ThreadPoolConfiguration threadPoolConfiguration) {
    ThreadPoolExecutor threadPoolExecutor;
    if (executorType.equals(ExecutorType.SEARCH)) {
      logger.info(
          "Creating LuceneSearchExecutor of size "
              + threadPoolConfiguration.getMaxSearchingThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(threadPoolConfiguration.getMaxSearchBufferedItems());
      // same as Executors.newFixedThreadPool except we want a NamedThreadFactory instead of
      // defaultFactory
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMaxSearchingThreads(),
              threadPoolConfiguration.getMaxSearchingThreads(),
              0,
              TimeUnit.SECONDS,
              docsToIndex,
              new NamedThreadFactory("LuceneSearchExecutor"));

    } else if (executorType.equals(ExecutorType.INDEX)) {
      logger.info(
          "Creating LuceneIndexingExecutor of size "
              + threadPoolConfiguration.getMaxIndexingThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(threadPoolConfiguration.getMaxIndexingBufferedItems());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMaxIndexingThreads(),
              threadPoolConfiguration.getMaxIndexingThreads(),
              0,
              TimeUnit.SECONDS,
              docsToIndex,
              new NamedThreadFactory("LuceneIndexingExecutor"));
    } else if (executorType.equals(ExecutorType.LUCENESERVER)) {
      logger.info(
          "Creating GrpcLuceneServerExecutor of size "
              + threadPoolConfiguration.getMaxGrpcLuceneserverThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(
              threadPoolConfiguration.getMaxGrpcLuceneserverBufferedItems());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMaxGrpcLuceneserverThreads(),
              threadPoolConfiguration.getMaxGrpcLuceneserverThreads(),
              0,
              TimeUnit.SECONDS,
              docsToIndex,
              new NamedThreadFactory("GrpcLuceneServerExecutor"));
    } else if (executorType.equals(ExecutorType.REPLICATIONSERVER)) {
      logger.info(
          "Creating GrpcReplicationServerExecutor of size "
              + threadPoolConfiguration.getMaxGrpcReplicationserverThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(
              threadPoolConfiguration.getMaxGrpcReplicationserverBufferedItems());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMaxGrpcReplicationserverThreads(),
              threadPoolConfiguration.getMaxGrpcReplicationserverThreads(),
              0,
              TimeUnit.SECONDS,
              docsToIndex,
              new NamedThreadFactory("GrpcReplicationServerExecutor"));
    } else if (executorType.equals(ExecutorType.FETCH)) {
      logger.info(
          "Creating LuceneFetchExecutor of size " + threadPoolConfiguration.getMaxFetchThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(threadPoolConfiguration.getMaxSearchBufferedItems());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMaxFetchThreads(),
              threadPoolConfiguration.getMaxFetchThreads(),
              0,
              TimeUnit.SECONDS,
              docsToIndex,
              new NamedThreadFactory("LuceneFetchExecutor"));
    } else if (executorType == ExecutorType.GRPC) {
      logger.info(
          "Creating default gRPC executor of size {}",
          threadPoolConfiguration.getGrpcExecutorThreads());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getGrpcExecutorThreads(),
              threadPoolConfiguration.getGrpcExecutorThreads(),
              0L,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE),
              new NamedThreadFactory("DefaultGrpcExecutor"));
    } else if (executorType == ExecutorType.METRICS) {
      logger.info(
          "Creating MetricsExecutor of size {}",
          threadPoolConfiguration.getMetricsExecutorThreads());
      threadPoolExecutor =
          new ThreadPoolExecutor(
              threadPoolConfiguration.getMetricsExecutorThreads(),
              threadPoolConfiguration.getMetricsExecutorThreads(),
              0L,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE),
              new NamedThreadFactory("MetricsExecutor"));
    } else {
      throw new RuntimeException("Invalid executor type provided " + executorType);
    }
    ThreadPoolCollector.addPool(executorType.name(), threadPoolExecutor);
    return threadPoolExecutor;
  }
}
