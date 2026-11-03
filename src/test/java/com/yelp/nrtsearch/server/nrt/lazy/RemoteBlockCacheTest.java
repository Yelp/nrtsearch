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
import java.util.List;
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
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> cacheTypes() {
    return List.of(new Object[] {"memory"}, new Object[] {"disk"});
  }

  @Parameterized.Parameter public String cacheType;

  private RemoteBlockCache cache(long maxBytes) throws IOException {
    return cacheType.equals("memory")
        ? new RemoteBlockCache.Memory(maxBytes, 10)
        : new RemoteBlockCache.Disk(folder.getRoot().toPath().resolve("cache"), maxBytes, 10);
  }

  private static byte[] block(int value) {
    byte[] b = new byte[10];
    java.util.Arrays.fill(b, (byte) value);
    return b;
  }

  @Test
  public void testHitsMissesAndLruEviction() throws Exception {
    RemoteBlockCache cache = cache(30);
    AtomicInteger loads = new AtomicInteger();
    for (int i = 0; i < 3; i++) {
      int v = i;
      cache.getBlock(
          "f",
          i,
          () -> {
            loads.incrementAndGet();
            return block(v);
          });
    }
    assertEquals(3, cache.getMisses());
    assertEquals(30, cache.getCurrentBytes());
    assertEquals(3, cache.getBlockCount());

    // touch block 0 so block 1 is the least recently used
    assertArrayEquals(
        block(0),
        cache.getBlock(
            "f",
            0,
            () -> {
              throw new AssertionError();
            }));
    assertEquals(1, cache.getHits());

    cache.getBlock("f", 3, () -> block(3));
    assertEquals(1, cache.getEvictions());
    assertEquals(30, cache.getCurrentBytes());
    // block 1 was evicted and is loaded again, block 0 is still cached
    cache.getBlock(
        "f",
        1,
        () -> {
          loads.incrementAndGet();
          return block(1);
        });
    assertEquals(4, loads.get());
    cache.getBlock(
        "f",
        0,
        () -> {
          throw new AssertionError("block 0 should be cached");
        });
    // blocks 0-3, then block 1 again
    assertEquals(50, cache.getRemoteBytes());
  }

  @Test
  public void testConcurrentMissesLoadOnce() throws Exception {
    RemoteBlockCache cache = cache(1000);
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Future<byte[]>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < 8; i++) {
        futures.add(
            executor.submit(
                () ->
                    cache.getBlock(
                        "f",
                        7,
                        () -> {
                          loads.incrementAndGet();
                          try {
                            release.await();
                          } catch (InterruptedException e) {
                            throw new IOException(e);
                          }
                          return block(7);
                        })));
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

  @Test
  public void testLoadErrorsCountedAndNotCached() throws Exception {
    RemoteBlockCache cache = cache(100);
    assertThrows(
        IOException.class,
        () ->
            cache.getBlock(
                "f",
                0,
                () -> {
                  throw new IOException("boom");
                }));
    assertEquals(1, cache.getLoadErrors());
    assertEquals(0, cache.getBlockCount());
    assertArrayEquals(block(1), cache.getBlock("f", 0, () -> block(1)));
    assertTrue(cache.getRemoteLoadNanos() > 0);
  }

  @Test
  public void testInvalidate() throws Exception {
    RemoteBlockCache cache = cache(100);
    cache.getBlock("a", 0, () -> block(1));
    cache.getBlock("a", 1, () -> block(2));
    cache.getBlock("b", 0, () -> block(3));
    cache.invalidate("a");
    assertEquals(1, cache.getBlockCount());
    assertEquals(10, cache.getCurrentBytes());
    if (cacheType.equals("disk")) {
      try (Stream<Path> files = Files.list(folder.getRoot().toPath().resolve("cache"))) {
        assertEquals(1, files.count());
      }
    }
  }

  @Test
  public void testCreateFromConfig() throws Exception {
    Path dir = folder.getRoot().toPath().resolve("default_dir");
    RemoteBlockCache memory =
        RemoteBlockCache.create(
            new LazyRemoteDataConfig(
                true, List.of("vec"), LazyRemoteDataConfig.CacheType.MEMORY, "", 2, 64),
            dir);
    assertEquals("memory", memory.getType());
    assertEquals(2L * 1024 * 1024, memory.getMaxBytes());
    assertEquals(64 * 1024, memory.getBlockSize());
    assertTrue(!Files.exists(dir));

    RemoteBlockCache disk =
        RemoteBlockCache.create(
            new LazyRemoteDataConfig(
                true, List.of("vec"), LazyRemoteDataConfig.CacheType.DISK, "", 2, 64),
            dir);
    assertEquals("disk", disk.getType());
    assertTrue(Files.isDirectory(dir));
  }
}
