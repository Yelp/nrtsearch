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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LazyRemoteDataConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RemoteBlockCacheTest {
  private static final int BLOCK = 10;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> cacheTypes() {
    return List.of(new Object[] {"memory"}, new Object[] {"off_heap"}, new Object[] {"disk"});
  }

  @Parameterized.Parameter public String cacheType;

  private RemoteBlockCache cache(long maxBytes) throws IOException {
    return switch (cacheType) {
      case "memory" -> new RemoteBlockCache.Memory(maxBytes, BLOCK);
      // two slots per slab, to exercise more than one slab
      case "off_heap" -> new RemoteBlockCache.OffHeap(maxBytes, BLOCK, 2 * BLOCK);
      case "disk" ->
          new RemoteBlockCache.Disk(folder.getRoot().toPath().resolve("cache"), maxBytes, BLOCK);
      default -> throw new IllegalArgumentException(cacheType);
    };
  }

  private static byte[] block(int value) {
    byte[] b = new byte[BLOCK];
    Arrays.fill(b, (byte) value);
    return b;
  }

  /** Read a whole block. */
  private static byte[] get(RemoteBlockCache cache, String file, long block, int value)
      throws IOException {
    byte[] out = new byte[BLOCK];
    cache.read(file, block, 0, out, 0, BLOCK, () -> block(value));
    return out;
  }

  private static void getCached(RemoteBlockCache cache, String file, long block)
      throws IOException {
    cache.read(
        file,
        block,
        0,
        new byte[BLOCK],
        0,
        BLOCK,
        () -> {
          throw new AssertionError("block " + block + " should be cached");
        });
  }

  @Test
  public void testHitsMissesAndLruEviction() throws Exception {
    RemoteBlockCache cache = cache(3 * BLOCK);
    for (int i = 0; i < 3; i++) {
      assertArrayEquals(block(i), get(cache, "f", i, i));
    }
    assertEquals(3, cache.getMisses());
    assertEquals(30, cache.getCurrentBytes());
    assertEquals(3, cache.getBlockCount());

    // touch block 0 so block 1 is the least recently used
    getCached(cache, "f", 0);
    assertEquals(1, cache.getHits());

    get(cache, "f", 3, 3);
    assertEquals(1, cache.getEvictions());
    assertEquals(30, cache.getCurrentBytes());
    // block 1 was evicted and is loaded again, block 0 is still cached
    AtomicInteger loads = new AtomicInteger();
    cache.read(
        "f",
        1,
        0,
        new byte[BLOCK],
        0,
        BLOCK,
        () -> {
          loads.incrementAndGet();
          return block(1);
        });
    assertEquals(1, loads.get());
    getCached(cache, "f", 0);
    // blocks 0-3, then block 1 again
    assertEquals(50, cache.getRemoteBytes());
  }

  @Test
  public void testPartialReads() throws Exception {
    RemoteBlockCache cache = cache(3 * BLOCK);
    byte[] data = new byte[BLOCK];
    for (int i = 0; i < BLOCK; i++) {
      data[i] = (byte) i;
    }
    byte[] out = new byte[6];
    // miss copies from the loaded data
    cache.read("f", 0, 3, out, 1, 4, () -> data.clone());
    assertArrayEquals(new byte[] {0, 3, 4, 5, 6, 0}, out);
    // hit copies from the stored block
    out = new byte[3];
    cache.read(
        "f",
        0,
        7,
        out,
        0,
        3,
        () -> {
          throw new AssertionError();
        });
    assertArrayEquals(new byte[] {7, 8, 9}, out);
  }

  @Test
  public void testShortLastBlock() throws Exception {
    RemoteBlockCache cache = cache(3 * BLOCK);
    byte[] out = new byte[4];
    cache.read("f", 5, 0, out, 0, 4, () -> new byte[] {1, 2, 3, 4});
    cache.read(
        "f",
        5,
        0,
        out,
        0,
        4,
        () -> {
          throw new AssertionError();
        });
    assertArrayEquals(new byte[] {1, 2, 3, 4}, out);
    // off heap uses a whole slot for a short block
    assertEquals(cacheType.equals("off_heap") ? BLOCK : 4, cache.getCurrentBytes());
    assertThrows(
        IllegalArgumentException.class,
        () -> cache.read("f", 6, 2, new byte[4], 0, 4, () -> new byte[] {1, 2, 3, 4}));
  }

  @Test
  public void testConcurrentMissesLoadOnce() throws Exception {
    RemoteBlockCache cache = cache(1000);
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Future<byte[]>> futures = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        futures.add(
            executor.submit(
                () -> {
                  byte[] out = new byte[BLOCK];
                  cache.read(
                      "f",
                      7,
                      0,
                      out,
                      0,
                      BLOCK,
                      () -> {
                        loads.incrementAndGet();
                        try {
                          release.await();
                        } catch (InterruptedException e) {
                          throw new IOException(e);
                        }
                        return block(7);
                      });
                  return out;
                }));
      }
      Thread.sleep(100);
      release.countDown();
      for (Future<byte[]> f : futures) {
        assertArrayEquals(block(7), f.get(10, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(1, loads.get());
    assertEquals(1, cache.getMisses());
    assertEquals(7, cache.getHits());
  }

  /**
   * Many threads reading many blocks through a small cache, so blocks are evicted and storage is
   * reused while other threads read. Every read must return the right block's bytes.
   */
  @Test
  public void testConcurrentReadsWithEviction() throws Exception {
    RemoteBlockCache cache = cache(4 * BLOCK);
    int threads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        int seed = t;
        futures.add(
            executor.submit(
                () -> {
                  Random random = new Random(seed);
                  byte[] out = new byte[BLOCK];
                  for (int i = 0; i < 2000; i++) {
                    int b = random.nextInt(20);
                    cache.read("f", b, 0, out, 0, BLOCK, () -> block(b));
                    assertArrayEquals("block " + b, block(b), out);
                  }
                  return null;
                }));
      }
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    assertTrue(cache.getEvictions() > 0);
    assertTrue(cache.getCurrentBytes() <= 4 * BLOCK);
  }

  @Test
  public void testLoadErrorsCountedAndNotCached() throws Exception {
    RemoteBlockCache cache = cache(100);
    assertThrows(
        IOException.class,
        () ->
            cache.read(
                "f",
                0,
                0,
                new byte[BLOCK],
                0,
                BLOCK,
                () -> {
                  throw new IOException("boom");
                }));
    assertEquals(1, cache.getLoadErrors());
    assertEquals(0, cache.getBlockCount());
    assertEquals(0, cache.getCurrentBytes());
    assertArrayEquals(block(1), get(cache, "f", 0, 1));
    assertTrue(cache.getRemoteLoadNanos() > 0);
  }

  @Test
  public void testInvalidate() throws Exception {
    RemoteBlockCache cache = cache(100);
    get(cache, "a", 0, 1);
    get(cache, "a", 1, 2);
    get(cache, "b", 0, 3);
    cache.invalidate("a");
    assertEquals(1, cache.getBlockCount());
    assertEquals(10, cache.getCurrentBytes());
    getCached(cache, "b", 0);
    if (cacheType.equals("disk")) {
      try (Stream<Path> files = Files.list(folder.getRoot().toPath().resolve("cache"))) {
        assertEquals(1, files.count());
      }
    }
    // freed storage is reused
    for (int i = 0; i < 9; i++) {
      assertArrayEquals(block(i), get(cache, "c", i, i));
    }
    assertEquals(100, cache.getCurrentBytes());
  }

  @Test
  public void testCreateFromConfig() throws Exception {
    Path dir = folder.getRoot().toPath().resolve("default_dir");
    for (LazyRemoteDataConfig.CacheType type : LazyRemoteDataConfig.CacheType.values()) {
      RemoteBlockCache cache =
          RemoteBlockCache.create(
              new LazyRemoteDataConfig(true, List.of("vec"), type, "", 2, 64), dir);
      assertEquals(type.name().toLowerCase(), cache.getType());
      assertEquals(2L * 1024 * 1024, cache.getMaxBytes());
      assertEquals(64 * 1024, cache.getBlockSize());
      assertEquals(type == LazyRemoteDataConfig.CacheType.DISK, Files.isDirectory(dir));
    }
  }
}
