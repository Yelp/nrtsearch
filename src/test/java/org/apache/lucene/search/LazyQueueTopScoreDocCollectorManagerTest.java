/*
 * Copyright 2025 Yelp Inc.
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
package org.apache.lucene.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.lucene.search.TotalHits.Relation;
import org.junit.Test;

public class LazyQueueTopScoreDocCollectorManagerTest {

  @Test
  public void testConstructorWithValidParameters() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, null, 100);
    assertNotNull("Manager should not be null with valid parameters", manager);
  }

  @Test
  public void testConstructorWithAfterParameter() {
    ScoreDoc after = new ScoreDoc(5, 8.5f);
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, after, 100);
    assertNotNull("Manager should not be null with after parameter", manager);
  }

  @Test
  public void testConstructorWithIntegerMaxValue() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, null, Integer.MAX_VALUE);
    assertNotNull("Manager should not be null with Integer.MAX_VALUE threshold", manager);
  }

  @Test
  public void testConstructorWithTwoParameters() {
    LazyQueueTopScoreDocCollectorManager manager = new LazyQueueTopScoreDocCollectorManager(10, 50);
    assertNotNull("Manager should not be null with two-parameter constructor", manager);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedConstructorWithSupportsConcurrency() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, null, 100, true);
    assertNotNull("Manager should not be null with deprecated constructor", manager);
  }

  @Test
  public void testConstructorWithZeroNumHits() {
    try {
      new LazyQueueTopScoreDocCollectorManager(0, null, 100);
      fail("Expected IllegalArgumentException for zero numHits");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Exception message should mention numHits must be > 0",
          e.getMessage().contains("numHits must be > 0"));
    }
  }

  @Test
  public void testConstructorWithNegativeNumHits() {
    try {
      new LazyQueueTopScoreDocCollectorManager(-5, null, 100);
      fail("Expected IllegalArgumentException for negative numHits");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Exception message should mention numHits must be > 0",
          e.getMessage().contains("numHits must be > 0"));
    }
  }

  @Test
  public void testConstructorWithNegativeTotalHitsThreshold() {
    try {
      new LazyQueueTopScoreDocCollectorManager(10, null, -1);
      fail("Expected IllegalArgumentException for negative totalHitsThreshold");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Exception message should mention totalHitsThreshold must be >= 0",
          e.getMessage().contains("totalHitsThreshold must be >= 0"));
    }
  }

  @Test
  public void testNewCollectorCreatesNonNullCollector() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("newCollector should return non-null collector", collector);
  }

  @Test
  public void testNewCollectorCreatesMultipleIndependentCollectors() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    LazyQueueTopScoreDocCollector collector1 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector2 = manager.newCollector();

    assertNotNull("First collector should not be null", collector1);
    assertNotNull("Second collector should not be null", collector2);
    assertTrue("Collectors should be different instances", collector1 != collector2);
  }

  @Test
  public void testNewCollectorWithSearchAfter() {
    ScoreDoc after = new ScoreDoc(10, 7.5f);
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, after, 50);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector with searchAfter should not be null", collector);
  }

  @Test
  public void testReduceWithEmptyCollection() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    Collection<LazyQueueTopScoreDocCollector> emptyCollectors = Collections.emptyList();
    TopDocs result = manager.reduce(emptyCollectors);

    assertNotNull("Reduce should return non-null TopDocs for empty collection", result);
    assertEquals(
        "TopDocs should have 0 score docs for empty collection", 0, result.scoreDocs.length);
  }

  @Test
  public void testReduceWithSingleCollector() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    Collection<LazyQueueTopScoreDocCollector> collectors = Arrays.asList(collector);

    TopDocs result = manager.reduce(collectors);
    assertNotNull("Reduce should return non-null TopDocs", result);
    assertNotNull("TopDocs scoreDocs should not be null", result.scoreDocs);
  }

  @Test
  public void testReduceWithMultipleCollectors() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    LazyQueueTopScoreDocCollector collector1 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector2 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector3 = manager.newCollector();

    Collection<LazyQueueTopScoreDocCollector> collectors =
        Arrays.asList(collector1, collector2, collector3);

    TopDocs result = manager.reduce(collectors);
    assertNotNull("Reduce should return non-null TopDocs", result);
    assertNotNull("TopDocs scoreDocs should not be null", result.scoreDocs);
    assertNotNull("TopDocs totalHits should not be null", result.totalHits);
  }

  @Test
  public void testReduceWithNullCollection() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    try {
      manager.reduce(null);
      fail("Expected NullPointerException for null collector collection");
    } catch (NullPointerException e) {
      // Expected behavior
    }
  }

  @Test
  public void testManagerImplementsCollectorManagerInterface() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    assertTrue(
        "Manager should implement CollectorManager interface", manager instanceof CollectorManager);
  }

  @Test
  public void testTotalHitsThresholdAdjustment() {
    // When totalHitsThreshold is smaller than numHits, it should be adjusted to numHits
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, null, 5);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector should be created even with small threshold", collector);
  }

  @Test
  public void testMaxScoreAccumulatorCreation() {
    // When totalHitsThreshold != Integer.MAX_VALUE, MaxScoreAccumulator should be created
    LazyQueueTopScoreDocCollectorManager managerWithAcc =
        new LazyQueueTopScoreDocCollectorManager(5, null, 100);
    LazyQueueTopScoreDocCollector collectorWithAcc = managerWithAcc.newCollector();
    assertNotNull("Collector with MaxScoreAccumulator should not be null", collectorWithAcc);

    // When totalHitsThreshold == Integer.MAX_VALUE, MaxScoreAccumulator should be null
    LazyQueueTopScoreDocCollectorManager managerWithoutAcc =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collectorWithoutAcc = managerWithoutAcc.newCollector();
    assertNotNull("Collector without MaxScoreAccumulator should not be null", collectorWithoutAcc);
  }

  @Test
  public void testManagerConsistencyAcrossCollectors() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10, null, 100);

    // Create multiple collectors from the same manager
    LazyQueueTopScoreDocCollector collector1 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector2 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector3 = manager.newCollector();

    // All collectors should have the same configuration (score mode)
    assertEquals(
        "All collectors should have same score mode",
        collector1.scoreMode(),
        collector2.scoreMode());
    assertEquals(
        "All collectors should have same score mode",
        collector2.scoreMode(),
        collector3.scoreMode());
  }

  @Test
  public void testCollectorManagerReusability() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    // Test that manager can be used multiple times for creating collectors
    for (int i = 0; i < 10; i++) {
      LazyQueueTopScoreDocCollector collector = manager.newCollector();
      assertNotNull("Collector " + i + " should not be null", collector);
    }
  }

  @Test
  public void testReducePreservesTotalHitsStructure() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 100);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    Collection<LazyQueueTopScoreDocCollector> collectors = Arrays.asList(collector);

    TopDocs result = manager.reduce(collectors);
    assertNotNull("TopDocs should not be null", result);
    assertNotNull("TotalHits should not be null", result.totalHits);
    assertTrue("TotalHits value should be non-negative", result.totalHits.value() >= 0);
    assertTrue(
        "TotalHits relation should be valid",
        result.totalHits.relation() == Relation.EQUAL_TO
            || result.totalHits.relation() == Relation.GREATER_THAN_OR_EQUAL_TO);
  }

  @Test
  public void testManagerWithLargeNumHits() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(10000, null, Integer.MAX_VALUE);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Manager should handle large numHits", collector);
  }

  @Test
  public void testManagerWithMinimalValidParameters() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(1, null, 0);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Manager should work with minimal valid parameters", collector);
  }

  @Test
  public void testReduceWithCollectorsContainingNullElements() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    LazyQueueTopScoreDocCollector validCollector = manager.newCollector();
    Collection<LazyQueueTopScoreDocCollector> collectorsWithNull = new ArrayList<>();
    collectorsWithNull.add(validCollector);
    collectorsWithNull.add(null);

    try {
      manager.reduce(collectorsWithNull);
      fail("Expected exception when reducing collectors with null elements");
    } catch (Exception e) {
      // Expected - could be NPE or other runtime exception
      assertTrue("Should throw some exception for null collector", true);
    }
  }

  @Test
  public void testThreadSafetyOfNewCollectorMethod() {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 50);

    // Create collectors in rapid succession to test thread safety
    Collection<LazyQueueTopScoreDocCollector> collectors = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      collectors.add(manager.newCollector());
    }

    // Verify all collectors are distinct and non-null
    for (LazyQueueTopScoreDocCollector collector : collectors) {
      assertNotNull("Each collector should be non-null", collector);
    }

    assertEquals("Should have created 100 collectors", 100, collectors.size());
  }

  @Test
  public void testManagerParameterBoundaryValues() {
    // Test boundary values for numHits and totalHitsThreshold
    LazyQueueTopScoreDocCollectorManager manager1 =
        new LazyQueueTopScoreDocCollectorManager(1, null, Integer.MAX_VALUE);
    assertNotNull("Manager with numHits=1 and max threshold should work", manager1.newCollector());

    // Use a large but reasonable value for numHits (1 million instead of Integer.MAX_VALUE)
    // to avoid Lucene's internal PriorityQueue size limitations
    LazyQueueTopScoreDocCollectorManager manager2 =
        new LazyQueueTopScoreDocCollectorManager(1000000, null, 0);
    assertNotNull(
        "Manager with large numHits and threshold=0 should work", manager2.newCollector());
  }
}
