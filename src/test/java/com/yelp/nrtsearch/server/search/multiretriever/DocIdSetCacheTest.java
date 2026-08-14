/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.multiretriever;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.junit.Test;

public class DocIdSetCacheTest {

  @Test
  public void testSingleRetrieverMerge() {
    DocIdSetCache cache = new DocIdSetCache(100);
    DocIdSetCache.RetrieverBitSets rbs = cache.newRetrieverBitSets();

    SparseFixedBitSet bitSet = rbs.getOrCreateBitSet(0, 1000);
    assertNotNull(bitSet);

    bitSet.set(5);
    rbs.recordHit();
    bitSet.set(10);
    rbs.recordHit();

    cache.merge();
    assertFalse(cache.isExceeded());

    BitSet merged = cache.getBitSet(0);
    assertNotNull(merged);
    assertTrue(merged.get(5));
    assertTrue(merged.get(10));
  }

  @Test
  public void testMultipleRetrieversMergeWithOverlap() {
    DocIdSetCache cache = new DocIdSetCache(100);

    DocIdSetCache.RetrieverBitSets rbs1 = cache.newRetrieverBitSets();
    DocIdSetCache.RetrieverBitSets rbs2 = cache.newRetrieverBitSets();

    SparseFixedBitSet bs1 = rbs1.getOrCreateBitSet(0, 100);
    bs1.set(5);
    rbs1.recordHit();
    bs1.set(10);
    rbs1.recordHit();

    SparseFixedBitSet bs2 = rbs2.getOrCreateBitSet(0, 100);
    bs2.set(10);
    rbs2.recordHit();
    bs2.set(20);
    rbs2.recordHit();

    cache.merge();

    BitSet merged = cache.getBitSet(0);
    assertTrue(merged.get(5));
    assertTrue(merged.get(10));
    assertTrue(merged.get(20));
    assertFalse(merged.get(0));
  }

  @Test
  public void testMultipleSegments() {
    DocIdSetCache cache = new DocIdSetCache(100);
    DocIdSetCache.RetrieverBitSets rbs = cache.newRetrieverBitSets();

    rbs.getOrCreateBitSet(0, 500).set(1);
    rbs.recordHit();
    rbs.getOrCreateBitSet(1, 300).set(2);
    rbs.recordHit();

    cache.merge();

    assertNotNull(cache.getBitSet(0));
    assertNotNull(cache.getBitSet(1));
    assertTrue(cache.getBitSet(0).get(1));
    assertTrue(cache.getBitSet(1).get(2));
  }

  @Test
  public void testPerRetrieverThresholdTripsEarlyExit() {
    // perRetrieverMaxHits=3 — exceeds at >3, i.e. 4th hit trips it
    DocIdSetCache cache = new DocIdSetCache(3);
    DocIdSetCache.RetrieverBitSets rbs = cache.newRetrieverBitSets();

    SparseFixedBitSet bitSet = rbs.getOrCreateBitSet(0, 100);

    for (int i = 0; i < 4; i++) {
      bitSet.set(i);
      rbs.recordHit();
    }

    assertTrue(rbs.isExceeded());
    assertTrue(cache.isExceeded());
    cache.merge();
  }

  @Test
  public void testExactlyAtPerRetrieverThreshold() {
    // perRetrieverMaxHits=4 — 4 hits is NOT > 4, so no early exit
    DocIdSetCache cache = new DocIdSetCache(4);
    DocIdSetCache.RetrieverBitSets rbs = cache.newRetrieverBitSets();

    SparseFixedBitSet bitSet = rbs.getOrCreateBitSet(0, 100);
    for (int i = 1; i <= 4; i++) {
      bitSet.set(i);
      rbs.recordHit();
    }

    assertFalse(rbs.isExceeded());
    cache.merge();
    assertFalse(cache.isExceeded());
  }

  @Test
  public void testGetBitSetReturnsNullForUnknownSegment() {
    DocIdSetCache cache = new DocIdSetCache(100);
    DocIdSetCache.RetrieverBitSets rbs = cache.newRetrieverBitSets();
    rbs.getOrCreateBitSet(0, 100).set(1);
    rbs.recordHit();
    cache.merge();

    assertNull(cache.getBitSet(1));
    assertNull(cache.getBitSet(99));
  }

  @Test
  public void testSecondRetrieverSeesExceededFromFirst() {
    DocIdSetCache cache = new DocIdSetCache(2);

    DocIdSetCache.RetrieverBitSets rbs1 = cache.newRetrieverBitSets();
    DocIdSetCache.RetrieverBitSets rbs2 = cache.newRetrieverBitSets();

    SparseFixedBitSet bs1 = rbs1.getOrCreateBitSet(0, 100);
    for (int i = 0; i < 5; i++) {
      bs1.set(i);
      rbs1.recordHit();
    }

    assertTrue(rbs2.isExceeded());
  }
}
