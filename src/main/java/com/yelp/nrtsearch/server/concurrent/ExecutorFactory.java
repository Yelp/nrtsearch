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
package com.yelp.nrtsearch.server.concurrent;

import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.monitoring.ThreadPoolCollector;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static Factory to generate {@link java.util.concurrent.ExecutorService} as per the {@link
 * ExecutorType} provided.
 */
public class ExecutorFactory {
  public enum ExecutorType {
    SEARCH,
    INDEX,
    SERVER,
    REPLICATIONSERVER,
    FETCH,
    GRPC,
    METRICS,
    VECTORMERGE,
    COMMIT
  }

  private static final Logger logger = LoggerFactory.getLogger(ExecutorFactory.class);

  private static ExecutorFactory instance;

  private final ThreadPoolConfiguration threadPoolConfiguration;
  private final Map<ExecutorType, ExecutorService> executorMap = new ConcurrentHashMap<>();

  /**
   * Initialize the factory with the provided {@link ThreadPoolConfiguration}.
   *
   * @param threadPoolConfiguration thread pool configuration
   */
  public static void init(ThreadPoolConfiguration threadPoolConfiguration) {
    instance = new ExecutorFactory(threadPoolConfiguration);
  }

  /**
   * Get the instance of the factory.
   *
   * @return instance of the factory
   * @throws IllegalStateException if the factory is not initialized
   */
  public static ExecutorFactory getInstance() {
    if (instance == null) {
      throw new IllegalStateException("ExecutorFactory not initialized");
    }
    return instance;
  }

  /**
   * Constructor to create the factory with the provided {@link ThreadPoolConfiguration}.
   *
   * @param threadPoolConfiguration thread pool configuration
   */
  public ExecutorFactory(ThreadPoolConfiguration threadPoolConfiguration) {
    this.threadPoolConfiguration = threadPoolConfiguration;
  }

  /**
   * Get the {@link ExecutorService} for the provided {@link ExecutorType}. The executor is cached,
   * so subsequent calls with the same {@link ExecutorType} will return the same executor.
   *
   * @param executorType {@link ExecutorType}
   * @return {@link ExecutorService}
   */
  public ExecutorService getExecutor(ExecutorType executorType) {
    return executorMap.computeIfAbsent(executorType, this::createExecutor);
  }

  private ExecutorService createExecutor(ExecutorType executorType) {
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
