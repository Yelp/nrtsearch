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

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import org.junit.Test;

public class ThreadPoolConfigurationTest {

  private YamlConfigReader getReaderForConfig(String config) {
    return new YamlConfigReader(new ByteArrayInputStream(config.getBytes()));
  }

  private int cpus() {
    return Runtime.getRuntime().availableProcessors();
  }

  @Test
  public void testConfiguration() throws FileNotFoundException {
    String config =
        Paths.get("src", "test", "resources", "config.yaml").toAbsolutePath().toString();
    NrtsearchConfig luceneServerConfiguration = new NrtsearchConfig(new FileInputStream(config));
    assertEquals("lucene_server_foo", luceneServerConfiguration.getNodeName());
    assertEquals("foohost", luceneServerConfiguration.getHostName());
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        luceneServerConfiguration
            .getThreadPoolConfiguration()
            .getThreadPoolSettings(ExecutorFactory.ExecutorType.SEARCH);
    assertEquals(threadPoolSettings.maxThreads(), 16);
    assertEquals(threadPoolSettings.maxBufferedItems(), 100);
  }

  @Test
  public void testSearchThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.SEARCH);
    assertEquals(
        threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_SEARCHING_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_SEARCH_BUFFERED_ITEMS);
    assertEquals("LuceneSearchExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testSearchThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  search:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.SEARCH);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testIndexThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.INDEX);
    assertEquals(threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_INDEXING_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_INDEXING_BUFFERED_ITEMS);
    assertEquals("LuceneIndexingExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testIndexThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  index:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.INDEX);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testLuceneServerThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.LUCENESERVER);
    assertEquals(
        threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_GRPC_LUCENESERVER_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_GRPC_LUCENESERVER_BUFFERED_ITEMS);
    assertEquals("GrpcLuceneServerExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testLuceneServerThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  luceneserver:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.LUCENESERVER);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testReplicationServerThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(
            ExecutorFactory.ExecutorType.REPLICATIONSERVER);
    assertEquals(
        threadPoolSettings.maxThreads(),
        ThreadPoolConfiguration.DEFAULT_GRPC_REPLICATIONSERVER_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_GRPC_REPLICATIONSERVER_BUFFERED_ITEMS);
    assertEquals("GrpcReplicationServerExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testReplicationServerThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  replicationserver:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(
            ExecutorFactory.ExecutorType.REPLICATIONSERVER);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testFetchThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.FETCH);
    assertEquals(threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_FETCH_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_FETCH_BUFFERED_ITEMS);
    assertEquals("LuceneFetchExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testFetchThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  fetch:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.FETCH);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testGrpcThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.GRPC);
    assertEquals(threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_GRPC_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(), ThreadPoolConfiguration.DEFAULT_GRPC_BUFFERED_ITEMS);
    assertEquals("GrpcExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testGrpcThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  grpc:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.GRPC);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testMetricsThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.METRICS);
    assertEquals(threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_METRICS_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_METRICS_BUFFERED_ITEMS);
    assertEquals("MetricsExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testMetricsThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  metrics:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.METRICS);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testVectorMergeThreadPool_default() {
    String config = "nodeName: node1";
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.VECTORMERGE);
    assertEquals(
        threadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_VECTOR_MERGE_THREADS);
    assertEquals(
        threadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_VECTOR_MERGE_BUFFERED_ITEMS);
    assertEquals("VectorMergeExecutor", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testVectorMergeThreadPool_set() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  vectormerge:",
            "    maxThreads: 5",
            "    maxBufferedItems: 10",
            "    threadNamePrefix: customName");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings threadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.VECTORMERGE);
    assertEquals(threadPoolSettings.maxThreads(), 5);
    assertEquals(threadPoolSettings.maxBufferedItems(), 10);
    assertEquals("customName", threadPoolSettings.threadNamePrefix());
  }

  @Test
  public void partialOverride() {
    String config =
        String.join(
            "\n",
            "threadPoolConfiguration:",
            "  search:",
            "    maxThreads: 5",
            "    threadNamePrefix: customName",
            "  index:",
            "    maxBufferedItems: 14");
    ThreadPoolConfiguration threadPoolConfiguration =
        new ThreadPoolConfiguration(getReaderForConfig(config));
    ThreadPoolConfiguration.ThreadPoolSettings searchThreadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.SEARCH);
    assertEquals(searchThreadPoolSettings.maxThreads(), 5);
    assertEquals(
        searchThreadPoolSettings.maxBufferedItems(),
        ThreadPoolConfiguration.DEFAULT_SEARCH_BUFFERED_ITEMS);
    assertEquals("customName", searchThreadPoolSettings.threadNamePrefix());
    ThreadPoolConfiguration.ThreadPoolSettings indexThreadPoolSettings =
        threadPoolConfiguration.getThreadPoolSettings(ExecutorFactory.ExecutorType.INDEX);
    assertEquals(
        indexThreadPoolSettings.maxThreads(), ThreadPoolConfiguration.DEFAULT_INDEXING_THREADS);
    assertEquals(indexThreadPoolSettings.maxBufferedItems(), 14);
    assertEquals("LuceneIndexingExecutor", indexThreadPoolSettings.threadNamePrefix());
  }

  @Test
  public void testGetNumThreads_default() {
    String config = "other_key: other_value";
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(100, numThreads);
  }

  @Test
  public void testGetNumThreads_number() {
    String config = "threadTest: 7";
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(7, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuDefault() {
    String config = "threadTest: {}";
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(cpus(), numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMultiplier() {
    String config = String.join("\n", "threadTest:", "  multiplier: 2.5");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    int expectedNumThreads = (int) (cpus() * 2.5f);
    assertEquals(expectedNumThreads, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuOffset() {
    String config = String.join("\n", "threadTest:", "  offset: 2");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    int expectedNumThreads = cpus() + 2;
    assertEquals(expectedNumThreads, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMultiplierAndOffset() {
    String config = String.join("\n", "threadTest:", "  offset: 2", "  multiplier: 2.5");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    int expectedNumThreads = (int) (cpus() * 2.5f) + 2;
    assertEquals(expectedNumThreads, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMinDefault() {
    String config = String.join("\n", "threadTest:", "  multiplier: 0");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(1, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMinSet() {
    String config = String.join("\n", "threadTest:", "  multiplier: 0", "  min: 10");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(10, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMinInvalid() {
    String config = String.join("\n", "threadTest:", "  multiplier: 0", "  min: 0");
    try {
      ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("min must be >= 1"));
    }
  }

  @Test
  public void testGetNumThreads_cpuMaxDefault() {
    String config = String.join("\n", "threadTest:", "  multiplier: 0", "  offset: 2147483647");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(2147483647, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMaxSet() {
    String config = String.join("\n", "threadTest:", "  multiplier: 100", "  max: 10");
    int numThreads =
        ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
    assertEquals(10, numThreads);
  }

  @Test
  public void testGetNumThreads_cpuMaxInvalid() {
    String config = String.join("\n", "threadTest:", "  max: 0");
    try {
      ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("max must be >= 1"));
    }
  }

  @Test
  public void testGetNumThreads_cpuInvalidType() {
    String config = "threadTest: []";
    try {
      ThreadPoolConfiguration.getNumThreads(getReaderForConfig(config), "threadTest", 100);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Invalid thread pool config type: class java.util.ArrayList, key: threadTest",
          e.getMessage());
    }
  }
}
