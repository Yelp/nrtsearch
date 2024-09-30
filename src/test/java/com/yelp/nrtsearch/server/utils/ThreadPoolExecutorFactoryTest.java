/*
 * Copyright 2024 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import java.io.ByteArrayInputStream;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Test;

public class ThreadPoolExecutorFactoryTest {

  private void init() {
    init("nodeName: node1");
  }

  private void init(String config) {
    LuceneServerConfiguration luceneServerConfiguration =
        new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
    ThreadPoolExecutorFactory.init(luceneServerConfiguration.getThreadPoolConfiguration());
  }

  @Test
  public void testCachesThreadPoolExecutor() {
    init();
    ThreadPoolExecutor executor1 =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.SEARCH);
    ThreadPoolExecutor executor2 =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.SEARCH);
    assertSame(executor1, executor2);
  }

  @Test
  public void testSearchThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.SEARCH);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_SEARCHING_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_SEARCH_BUFFERED_ITEMS);
  }

  @Test
  public void testSearchThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  search:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.SEARCH);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testIndexThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.INDEX);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_INDEXING_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_INDEXING_BUFFERED_ITEMS);
  }

  @Test
  public void testIndexThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  index:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.INDEX);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testLuceneServerThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.LUCENESERVER);
    assertEquals(
        executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_GRPC_LUCENESERVER_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_GRPC_LUCENESERVER_BUFFERED_ITEMS);
  }

  @Test
  public void testLuceneServerThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  luceneserver:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.LUCENESERVER);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testReplicationServerThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER);
    assertEquals(
        executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_GRPC_REPLICATIONSERVER_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS);
  }

  @Test
  public void testReplicationServerThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  replicationserver:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.REPLICATIONSERVER);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testFetchThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.FETCH);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_FETCH_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_FETCH_BUFFERED_ITEMS);
  }

  @Test
  public void testFetchThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  fetch:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.FETCH);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testGrpcThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.GRPC);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_GRPC_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_GRPC_BUFFERED_ITEMS);
  }

  @Test
  public void testGrpcThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  grpc:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.GRPC);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testMetricsThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.METRICS);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_METRICS_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_METRICS_BUFFERED_ITEMS);
  }

  @Test
  public void testMetricsThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  metrics:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.METRICS);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testVectorMergeThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.VECTORMERGE);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_VECTOR_MERGE_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_VECTOR_MERGE_BUFFERED_ITEMS);
  }

  @Test
  public void testVectorMergeThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  vectormerge:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.VECTORMERGE);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }
}
