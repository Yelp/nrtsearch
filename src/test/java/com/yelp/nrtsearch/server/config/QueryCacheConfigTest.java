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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.QueryCacheConfig.MinSegmentSizePredicate;
import java.io.ByteArrayInputStream;
import java.util.function.Predicate;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.Test;

public class QueryCacheConfigTest {

  private static QueryCacheConfig getConfig(String configFile) {
    return QueryCacheConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefault() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    QueryCacheConfig config = getConfig(configFile);
    assertTrue(config.getEnabled());
    assertEquals(config.getMaxQueries(), QueryCacheConfig.DEFAULT_MAX_QUERIES);
    assertEquals(config.getMaxMemoryBytes(), (32L * 1024 * 1024));
    assertEquals(config.getSkipCacheFactor(), QueryCacheConfig.DEFAULT_SKIP_CACHE_FACTOR, 0.0);
    Predicate<LeafReaderContext> leafPredicate = config.getLeafPredicate();
    assertTrue(leafPredicate instanceof QueryCacheConfig.MinSegmentSizePredicate);
    MinSegmentSizePredicate minSegmentSizePredicate = (MinSegmentSizePredicate) leafPredicate;
    assertEquals(minSegmentSizePredicate.minSize, QueryCacheConfig.DEFAULT_MIN_DOCS);
    assertEquals(
        minSegmentSizePredicate.minSizeRatio, QueryCacheConfig.DEFAULT_MIN_SIZE_RATIO, 0.0);
  }

  @Test
  public void testSetConfig() {
    String configFile =
        String.join(
            "\n",
            "nodeName: \"lucene_server_foo\"",
            "queryCache:",
            "  enabled: false",
            "  maxQueries: 5000",
            "  maxMemory: '128MB'",
            "  minDocs: 7000",
            "  minSizeRatio: 0.05",
            "  skipCacheFactor: 400");
    QueryCacheConfig config = getConfig(configFile);
    assertFalse(config.getEnabled());
    assertEquals(config.getMaxQueries(), 5000);
    assertEquals(config.getMaxMemoryBytes(), (128L * 1024 * 1024));
    assertEquals(config.getSkipCacheFactor(), 400.0, 0.0);
    Predicate<LeafReaderContext> leafPredicate = config.getLeafPredicate();
    assertTrue(leafPredicate instanceof QueryCacheConfig.MinSegmentSizePredicate);
    MinSegmentSizePredicate minSegmentSizePredicate = (MinSegmentSizePredicate) leafPredicate;
    assertEquals(minSegmentSizePredicate.minSize, 7000);
    assertEquals(minSegmentSizePredicate.minSizeRatio, 0.05, 0.0001);
  }

  @Test
  public void testConvertSizeStrAbsolute() {
    assertEquals(QueryCacheConfig.sizeStrToBytes("1111"), 1111L);
    assertEquals(QueryCacheConfig.sizeStrToBytes("222KB"), 222L * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("333kB"), 333L * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("444kb"), 444L * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("44MB"), 44L * 1024 * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("55mB"), 55L * 1024 * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("66mb"), 66L * 1024 * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("7GB"), 7L * 1024 * 1024 * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("8gB"), 8L * 1024 * 1024 * 1024);
    assertEquals(QueryCacheConfig.sizeStrToBytes("9gb"), 9L * 1024 * 1024 * 1024);

    assertEquals(QueryCacheConfig.sizeStrToBytes("1234.5KB"), (long) (1234.5 * 1024));
    assertEquals(QueryCacheConfig.sizeStrToBytes("55.5MB"), (long) (55.5 * 1024 * 1024));
  }

  @Test
  public void testConvertSizeStrPercentage() {
    long heapSize = Runtime.getRuntime().maxMemory();
    assertEquals(QueryCacheConfig.sizeStrToBytes("10%"), (long) (0.1 * heapSize));
    assertEquals(QueryCacheConfig.sizeStrToBytes("5.5%"), (long) (0.055 * heapSize));
  }

  @Test
  public void testConvertEmptySizeStr() {
    try {
      QueryCacheConfig.sizeStrToBytes("");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot convert size string: ", e.getMessage());
    }

    try {
      QueryCacheConfig.sizeStrToBytes("%");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot convert empty percentage", e.getMessage());
    }

    try {
      QueryCacheConfig.sizeStrToBytes("mb");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("For input string: \"mb\"", e.getMessage());
    }
  }
}
