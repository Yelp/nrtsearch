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
    REPLICATIONSERVER
  }

  private static final Logger logger =
      LoggerFactory.getLogger(ThreadPoolExecutorFactory.class.getName());

  /**
   * @param executorType {@link ExecutorType}
   * @param threadPoolConfiguration {@link ThreadPoolConfiguration}
   * @return {@link ThreadPoolExecutor}
   */
  public static ThreadPoolExecutor getThreadPoolExecutor(
      ExecutorType executorType, ThreadPoolConfiguration threadPoolConfiguration) {
    if (executorType.equals(ExecutorType.SEARCH)) {
      logger.info(
          "Creating LuceneSearchExecutor of size "
              + threadPoolConfiguration.getMaxSearchingThreads());
      BlockingQueue<Runnable> docsToIndex =
          new LinkedBlockingQueue<Runnable>(threadPoolConfiguration.getMaxSearchBufferedItems());
      // same as Executors.newFixedThreadPool except we want a NamedThreadFactory instead of
      // defaultFactory
      return new ThreadPoolExecutor(
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
      return new ThreadPoolExecutor(
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
      return new ThreadPoolExecutor(
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
      return new ThreadPoolExecutor(
          threadPoolConfiguration.getMaxGrpcReplicationserverThreads(),
          threadPoolConfiguration.getMaxGrpcReplicationserverThreads(),
          0,
          TimeUnit.SECONDS,
          docsToIndex,
          new NamedThreadFactory("GrpcReplicationServerExecutor"));
    } else {
      throw new RuntimeException("Invalid executor type provided " + executorType.toString());
    }
  }
}
