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
    LuceneServerConfiguration luceneServerConfiguration =
        new LuceneServerConfiguration(new FileInputStream(config));
    assertEquals("lucene_server_foo", luceneServerConfiguration.getNodeName());
    assertEquals("foohost", luceneServerConfiguration.getHostName());
    assertEquals(
        luceneServerConfiguration.getThreadPoolConfiguration().getMaxSearchingThreads(), 16);
    assertEquals(
        luceneServerConfiguration.getThreadPoolConfiguration().getMaxSearchBufferedItems(), 100);
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
