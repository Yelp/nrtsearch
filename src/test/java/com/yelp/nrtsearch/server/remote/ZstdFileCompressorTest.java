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
package com.yelp.nrtsearch.server.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class ZstdFileCompressorTest {

  private final FileCompressor compressor =
      new ZstdFileCompressor(ZstdFileCompressor.DEFAULT_LEVEL, ZstdFileCompressor.DEFAULT_WORKERS);

  @Test
  public void testCompressDecompress_roundTrip() throws IOException {
    byte[] original = "hello world, this is some test data to compress".getBytes();
    assertArrayEquals(original, decompress(compressor, compress(compressor, original)));
  }

  @Test
  public void testCompressDecompress_emptyData() throws IOException {
    byte[] original = new byte[0];
    assertArrayEquals(original, decompress(compressor, compress(compressor, original)));
  }

  @Test
  public void testCompressDecompress_largeData() throws IOException {
    byte[] original = new byte[1024 * 1024]; // 1MB
    Arrays.fill(original, (byte) 42);
    byte[] compressed = compress(compressor, original);
    assertTrue(compressed.length < original.length);
    assertArrayEquals(original, decompress(compressor, compressed));
  }

  @Test
  public void testCompressDecompress_incompressibleData() throws IOException {
    byte[] original = new byte[4 * 1024 * 1024];
    new Random(42).nextBytes(original);
    assertArrayEquals(original, decompress(compressor, compress(compressor, original)));
  }

  @Test
  public void testCompressDecompress_multipleWorkers() throws IOException {
    FileCompressor multiThreaded = new ZstdFileCompressor(3, 2);
    byte[] original = mixedData(8 * 1024 * 1024);
    assertArrayEquals(original, decompress(multiThreaded, compress(multiThreaded, original)));
  }

  @Test
  public void testCompressDecompress_negativeLevel() throws IOException {
    FileCompressor fast = new ZstdFileCompressor(-5, 0);
    byte[] original = mixedData(1024 * 1024);
    assertArrayEquals(original, decompress(fast, compress(fast, original)));
  }

  @Test
  public void testDecompressIndependentOfLevel() throws IOException {
    byte[] original = mixedData(1024 * 1024);
    byte[] compressed = compress(new ZstdFileCompressor(19, 0), original);
    assertArrayEquals(original, decompress(new ZstdFileCompressor(1, 0), compressed));
  }

  @Test
  public void testInvalidLevel() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ZstdFileCompressor(Zstd.maxCompressionLevel() + 1, 0));
    assertTrue(e.getMessage().contains("zstd level must be in"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ZstdFileCompressor(Zstd.minCompressionLevel() - 1, 0));
  }

  @Test
  public void testInvalidWorkers() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> new ZstdFileCompressor(3, -1));
    assertEquals("zstd workers must be >= 0, got: -1", e.getMessage());
  }

  /** Half compressible, half random data. */
  private static byte[] mixedData(int size) {
    byte[] data = new byte[size];
    Arrays.fill(data, 0, size / 2, (byte) 7);
    byte[] random = new byte[size - size / 2];
    new Random(17).nextBytes(random);
    System.arraycopy(random, 0, data, size / 2, random.length);
    return data;
  }

  private static byte[] compress(FileCompressor compressor, byte[] data) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream compStream = compressor.compressStream(baos)) {
      compStream.write(data);
    }
    return baos.toByteArray();
  }

  private static byte[] decompress(FileCompressor compressor, byte[] compressed)
      throws IOException {
    try (InputStream decompStream =
        compressor.decompressStream(new ByteArrayInputStream(compressed))) {
      return IOUtils.toByteArray(decompStream);
    }
  }
}
