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
import java.util.Map;
import java.util.concurrent.*;
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
    METRICS,
    VECTORMERGE
  }

  private static final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorFactory.class);

  private static ThreadPoolExecutorFactory instance;

  private final ThreadPoolConfiguration threadPoolConfiguration;
  private final Map<ExecutorType, ThreadPoolExecutor> executorMap = new ConcurrentHashMap<>();

  /**
   * Initialize the factory with the provided {@link ThreadPoolConfiguration}.
   *
   * @param threadPoolConfiguration thread pool configuration
   */
  public static void init(ThreadPoolConfiguration threadPoolConfiguration) {
    instance = new ThreadPoolExecutorFactory(threadPoolConfiguration);
  }

  /**
   * Get the instance of the factory.
   *
   * @return instance of the factory
   * @throws IllegalStateException if the factory is not initialized
   */
  public static ThreadPoolExecutorFactory getInstance() {
    if (instance == null) {
      throw new IllegalStateException("ThreadPoolExecutorFactory not initialized");
    }
    return instance;
  }

  /**
   * Constructor to create the factory with the provided {@link ThreadPoolConfiguration}.
   *
   * @param threadPoolConfiguration thread pool configuration
   */
  public ThreadPoolExecutorFactory(ThreadPoolConfiguration threadPoolConfiguration) {
    this.threadPoolConfiguration = threadPoolConfiguration;
  }

  /**
   * Get the {@link ThreadPoolExecutor} for the provided {@link ExecutorType}. The executor is
   * cached, so subsequent calls with the same {@link ExecutorType} will return the same executor.
   *
   * @param executorType {@link ExecutorType}
   * @return {@link ThreadPoolExecutor}
   */
  public ThreadPoolExecutor getThreadPoolExecutor(ExecutorType executorType) {
    return executorMap.computeIfAbsent(executorType, this::createThreadPoolExecutor);
  }

  private ThreadPoolExecutor createThreadPoolExecutor(ExecutorType executorType) {
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(executorType);
    logger.info(
        "Creating {} of size {}",
        threadPoolSettings.threadNamePrefix(),
        threadPoolSettings.maxThreads());
    BlockingQueue<Runnable> queue =
        new LinkedBlockingQueue<>(threadPoolSettings.maxBufferedItems());
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            threadPoolSettings.maxThreads(),
            threadPoolSettings.maxThreads(),
            0L,
            TimeUnit.SECONDS,
            queue,
            new NamedThreadFactory(threadPoolSettings.threadNamePrefix()));
    ThreadPoolCollector.addPool(executorType.name(), threadPoolExecutor);
    return threadPoolExecutor;
  }
}
