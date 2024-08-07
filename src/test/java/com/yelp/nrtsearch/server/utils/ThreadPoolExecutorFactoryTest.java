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
  public void testVectorMergeThreadPool_default() {
    init();
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.VECTOR_MERGE);
    assertEquals(
        executor.getCorePoolSize(), ThreadPoolConfiguration.DEFAULT_VECTOR_MERGE_EXECUTOR_THREADS);
    assertEquals(executor.getQueue().remainingCapacity(), 100);
  }

  @Test
  public void testVectorMergeThreadPool_set() {
    init(
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  vectorMergeThreads: 5",
            "  vectorMergeBufferedItems: 10"));
    ThreadPoolExecutor executor =
        ThreadPoolExecutorFactory.getInstance()
            .getThreadPoolExecutor(ThreadPoolExecutorFactory.ExecutorType.VECTOR_MERGE);
    assertEquals(executor.getCorePoolSize(), 5);
    assertEquals(executor.getQueue().remainingCapacity(), 10);
  }
}
