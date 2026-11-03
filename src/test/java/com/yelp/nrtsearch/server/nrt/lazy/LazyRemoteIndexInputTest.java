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

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.store.IndexInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LazyRemoteIndexInputTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final int BLOCK = 4096;
  private byte[] data;
  private final List<long[]> reads = new ArrayList<>();
  private RemoteBlockCache cache;

  @Before
  public void setUp() throws Exception {
    data = new byte[BLOCK * 10 + 123];
    new Random(1).nextBytes(data);
    cache = new RemoteBlockCache(folder.newFolder("cache").toPath(), BLOCK * 4L, BLOCK);
  }

  private IndexInput open(byte[] header, byte[] footer) {
    return new LazyRemoteIndexInput(
        "test",
        "file1",
        data.length,
        header,
        footer,
        (offset, length) -> {
          reads.add(new long[] {offset, length});
          return Arrays.copyOfRange(data, (int) offset, (int) offset + length);
        },
        cache);
  }

  private IndexInput open() {
    return open(Arrays.copyOf(data, 20), Arrays.copyOfRange(data, data.length - 16, data.length));
  }

  @Test
  public void testSequentialRead() throws Exception {
    try (IndexInput in = open()) {
      assertEquals(data.length, in.length());
      byte[] out = new byte[data.length];
      in.readBytes(out, 0, out.length);
      assertArrayEquals(data, out);
    }
    // every block read once, each as one range of at most one block
    assertEquals(11, reads.size());
    for (long[] r : reads) {
      assertTrue(r[1] <= BLOCK);
      assertEquals(0, r[0] % BLOCK);
    }
  }

  @Test
  public void testHeaderAndFooterNeedNoRemoteRead() throws Exception {
    try (IndexInput in = open()) {
      byte[] header = new byte[20];
      in.readBytes(header, 0, 20);
      assertArrayEquals(Arrays.copyOf(data, 20), header);
      in.seek(data.length - 16);
      byte[] footer = new byte[16];
      in.readBytes(footer, 0, 16);
      assertArrayEquals(Arrays.copyOfRange(data, data.length - 16, data.length), footer);
    }
    assertTrue(reads.isEmpty());
  }

  @Test
  public void testRandomAccessAndCaching() throws Exception {
    Random random = new Random(2);
    try (IndexInput in = open()) {
      for (int i = 0; i < 200; i++) {
        int pos = random.nextInt(data.length - 100);
        int len = 1 + random.nextInt(100);
        in.seek(pos);
        byte[] out = new byte[len];
        in.readBytes(out, 0, len);
        assertArrayEquals(Arrays.copyOfRange(data, pos, pos + len), out);
      }
    }
    // the cache holds 4 of 11 blocks; random reads hit the cache some of the time
    assertTrue(cache.getHits() > 0);
    assertTrue(cache.getCurrentBytes() <= BLOCK * 4L);
    assertTrue(cache.getEvictions() > 0);
  }

  @Test
  public void testReadsCachedAcrossInputs() throws Exception {
    try (IndexInput in = open()) {
      in.seek(BLOCK + 10);
      in.readByte();
    }
    int remoteReads = reads.size();
    try (IndexInput in = open()) {
      in.seek(BLOCK + 20);
      assertEquals(data[BLOCK + 20], in.readByte());
    }
    assertEquals(remoteReads, reads.size());
  }

  @Test
  public void testSliceAndClone() throws Exception {
    try (IndexInput in = open()) {
      IndexInput slice = in.slice("s", BLOCK - 5, 100);
      assertEquals(100, slice.length());
      byte[] out = new byte[100];
      slice.readBytes(out, 0, 100);
      assertArrayEquals(Arrays.copyOfRange(data, BLOCK - 5, BLOCK + 95), out);

      IndexInput nested = slice.slice("n", 10, 20);
      nested.seek(5);
      assertEquals(data[BLOCK - 5 + 10 + 5], nested.readByte());

      in.seek(500);
      IndexInput clone = in.clone();
      assertEquals(500, clone.getFilePointer());
      assertEquals(data[500], clone.readByte());

      assertThrows(EOFException.class, () -> slice.seek(101));
      assertThrows(IllegalArgumentException.class, () -> in.slice("bad", data.length - 5, 10));
    }
  }

  @Test
  public void testRandomAccessInput() throws Exception {
    try (IndexInput in = open()) {
      var rai = (org.apache.lucene.store.RandomAccessInput) in;
      assertEquals(data[3 * BLOCK + 7], rai.readByte(3 * BLOCK + 7));
    }
  }

  @Test
  public void testReadPastEof() throws Exception {
    try (IndexInput in = open()) {
      in.seek(data.length - 2);
      byte[] out = new byte[4];
      assertThrows(EOFException.class, () -> in.readBytes(out, 0, 4));
    }
  }

  @Test
  public void testNoHeaderFooter() throws Exception {
    try (IndexInput in = open(null, null)) {
      byte[] out = new byte[data.length];
      in.readBytes(out, 0, out.length);
      assertArrayEquals(data, out);
    }
  }

  @Test
  public void testCacheInvalidate() throws Exception {
    try (IndexInput in = open()) {
      in.seek(BLOCK * 2);
      in.readByte();
    }
    assertTrue(cache.getCurrentBytes() > 0);
    cache.invalidate("file1");
    assertEquals(0, cache.getCurrentBytes());
  }
}
