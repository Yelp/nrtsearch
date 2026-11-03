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
package com.yelp.nrtsearch.server.nrt.lazy;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import org.junit.Test;

/**
 * Tests specific to {@link RemoteBlockCache.OffHeap}; shared behavior is in RemoteBlockCacheTest.
 */
public class OffHeapBlockCacheTest {
  private static final int BLOCK = 1024;

  private static byte[] block(int value) {
    byte[] b = new byte[BLOCK];
    Arrays.fill(b, (byte) value);
    return b;
  }

  @Test
  public void testSlabsAllocatedAsCacheFills() throws Exception {
    // 10 slots, 4 per slab: slabs of 4, 4 and 2 slots
    RemoteBlockCache.OffHeap cache = new RemoteBlockCache.OffHeap(10 * BLOCK, BLOCK, 4 * BLOCK);
    assertEquals(0, cache.getAllocatedBytes());
    byte[] out = new byte[BLOCK];
    cache.read("f", 0, 0, out, 0, BLOCK, () -> block(0));
    assertEquals(4 * BLOCK, cache.getAllocatedBytes());
    for (int i = 1; i < 5; i++) {
      int v = i;
      cache.read("f", i, 0, out, 0, BLOCK, () -> block(v));
    }
    assertEquals(8 * BLOCK, cache.getAllocatedBytes());
    for (int i = 5; i < 30; i++) {
      int v = i;
      cache.read("f", i, 0, out, 0, BLOCK, () -> block(v));
    }
    // never more than the cache size, and evicted slots are reused
    assertEquals(10 * BLOCK, cache.getAllocatedBytes());
    assertEquals(10 * BLOCK, cache.getCurrentBytes());
    assertEquals(20, cache.getEvictions());
    for (int i = 20; i < 30; i++) {
      int v = i;
      cache.read(
          "f",
          i,
          0,
          out,
          0,
          BLOCK,
          () -> {
            throw new AssertionError("block " + v + " should be cached");
          });
      assertArrayEquals(block(i), out);
    }
  }

  @Test
  public void testCapacityRoundsDownToWholeSlots() {
    RemoteBlockCache.OffHeap cache = new RemoteBlockCache.OffHeap(BLOCK * 3 + 100, BLOCK);
    assertEquals(3L * BLOCK, cache.getMaxBytes());
    assertThrows(IllegalArgumentException.class, () -> new RemoteBlockCache.OffHeap(100, BLOCK));
  }

  @Test
  public void testReusedSlotIsNotReadAsOldBlock() throws Exception {
    // one slot, so every new block reuses it
    RemoteBlockCache.OffHeap cache = new RemoteBlockCache.OffHeap(BLOCK, BLOCK);
    byte[] out = new byte[BLOCK];
    cache.read("f", 0, 0, out, 0, BLOCK, () -> block(1));
    RemoteBlockCache.Entry entry = new RemoteBlockCache.Entry(BLOCK, BLOCK, 0);
    // a reader that looked up block 0 before it was evicted
    cache.read("f", 1, 0, out, 0, BLOCK, () -> block(2));
    // the slot now holds block 1, so reading it as block 0 is a miss, not block 1's data
    assertEquals(
        false, cache.copyStored(new RemoteBlockCache.BlockKey("f", 0), entry, 0, out, 0, BLOCK));
    assertEquals(
        true, cache.copyStored(new RemoteBlockCache.BlockKey("f", 1), entry, 0, out, 0, BLOCK));
    assertArrayEquals(block(2), out);
  }

  /**
   * Two slots and many threads reading many blocks, so nearly every read races with its slot being
   * reused. A torn read would return bytes from two blocks.
   */
  @Test
  public void testSlotReuseStress() throws Exception {
    RemoteBlockCache.OffHeap cache = new RemoteBlockCache.OffHeap(2 * BLOCK, BLOCK);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(8);
    try {
      java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
      for (int t = 0; t < 8; t++) {
        int seed = t;
        futures.add(
            executor.submit(
                () -> {
                  java.util.Random random = new java.util.Random(seed);
                  byte[] out = new byte[BLOCK];
                  for (int i = 0; i < 5000; i++) {
                    int b = random.nextInt(50);
                    cache.read("f", b, 0, out, 0, BLOCK, () -> block(b));
                    for (byte value : out) {
                      if (value != (byte) b) {
                        throw new AssertionError("block " + b + " read byte " + value);
                      }
                    }
                  }
                  return null;
                }));
      }
      for (java.util.concurrent.Future<?> f : futures) {
        f.get(60, java.util.concurrent.TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(2L * BLOCK, cache.getAllocatedBytes());
  }
}
