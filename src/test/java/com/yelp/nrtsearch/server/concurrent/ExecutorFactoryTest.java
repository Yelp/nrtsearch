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
package com.yelp.nrtsearch.server.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import java.io.ByteArrayInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.After;
import org.junit.Test;

public class ExecutorFactoryTest {
  ExecutorFactory executorFactory;

  private void init() {
    init("nodeName: node1");
  }

  private void init(String config) {
    NrtsearchConfig serverConfiguration =
        new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
    executorFactory = new ExecutorFactory(serverConfiguration.getThreadPoolConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    if (executorFactory != null) {
      executorFactory.close();
    }
    executorFactory = null;
  }

  @Test
  public void testCachesThreadPoolExecutor() {
    init();
    ExecutorService executor1 = executorFactory.getExecutor(ExecutorFactory.ExecutorType.SEARCH);
    ExecutorService executor2 = executorFactory.getExecutor(ExecutorFactory.ExecutorType.SEARCH);
    assertSame(executor1, executor2);
  }

  @Test
  public void testSearchThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.SEARCH);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.SEARCH);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testIndexThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.INDEX);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.INDEX);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testServerThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.SERVER);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_GRPC_SERVER_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_GRPC_SERVER_BUFFERED_ITEMS);
  }

  @Test
  public void testServerThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  server:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.SERVER);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testReplicationServerThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor)
            executorFactory.getExecutor(ExecutorFactory.ExecutorType.REPLICATIONSERVER);
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
        (ThreadPoolExecutor)
            executorFactory.getExecutor(ExecutorFactory.ExecutorType.REPLICATIONSERVER);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testFetchThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.FETCH);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.FETCH);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testGrpcThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.GRPC);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.GRPC);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testMetricsThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.METRICS);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.METRICS);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testVectorMergeThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.VECTORMERGE);
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
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.VECTORMERGE);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testCommitThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.COMMIT);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_COMMIT_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_COMMIT_BUFFERED_ITEMS);
  }

  @Test
  public void testCommitThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  commit:",
            "    maxThreads: 3",
            "    maxBufferedItems: 25"));
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.COMMIT);
    assertEquals(executor.getCorePoolSize(), 3);
    assertEquals(executor.getQueue().remainingCapacity(), 25);
  }

  @Test
  public void testRemoteThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE);
    assertEquals(executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS);
    assertEquals(
        executor.getQueue().remainingCapacity(),
        ThreadPoolConfiguration.DEFAULT_REMOTE_BUFFERED_ITEMS);
  }

  @Test
  public void testRemoteThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  remote:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10"));
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }

  @Test
  public void testVirtualThreadExecutor() {
    init(String.join("\n", "threadPoolConfiguration:", "  search:", "    useVirtualThreads: true"));
    ExecutorService executor = executorFactory.getExecutor(ExecutorFactory.ExecutorType.SEARCH);
    assertTrue(executor instanceof ExecutorServiceStatsWrapper);
  }
}
