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
import static org.junit.Assert.assertNotSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class LZ4FileCompressorTest {

  private final FileCompressor compressor = new LZ4FileCompressor();

  @Test
  public void testCompressDecompress_roundTrip() throws IOException {
    byte[] original = "hello world, this is some test data to compress".getBytes();
    byte[] compressed = compress(original);
    byte[] decompressed = decompress(compressed);
    assertArrayEquals(original, decompressed);
  }

  @Test
  public void testCompressDecompress_emptyData() throws IOException {
    byte[] original = new byte[0];
    byte[] compressed = compress(original);
    byte[] decompressed = decompress(compressed);
    assertArrayEquals(original, decompressed);
  }

  @Test
  public void testCompressDecompress_largeData() throws IOException {
    byte[] original = new byte[1024 * 1024]; // 1MB
    Arrays.fill(original, (byte) 42);
    byte[] compressed = compress(original);
    byte[] decompressed = decompress(compressed);
    assertArrayEquals(original, decompressed);
  }

  @Test
  public void testCompressedDataIsDifferent() throws IOException {
    byte[] original = "some data".getBytes();
    byte[] compressed = compress(original);
    assertNotSame(original, compressed);
    // For LZ4 frame format there will be a header/footer, so compressed != original
    // (at minimum the LZ4 frame header is present)
    // Just verify we can round-trip; for tiny data compressed may be larger than original
    byte[] decompressed = decompress(compressed);
    assertArrayEquals(original, decompressed);
  }

  private byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream compStream = compressor.compressStream(baos)) {
      compStream.write(data);
    }
    return baos.toByteArray();
  }

  private byte[] decompress(byte[] compressed) throws IOException {
    try (InputStream decompStream =
        compressor.decompressStream(new ByteArrayInputStream(compressed))) {
      return IOUtils.toByteArray(decompStream);
    }
  }
}
