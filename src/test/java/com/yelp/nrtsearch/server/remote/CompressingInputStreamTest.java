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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class CompressingInputStreamTest {

  private final FileCompressor compressor = new LZ4FileCompressor();

  private static class TrackingInputStream extends ByteArrayInputStream {
    boolean closed = false;

    TrackingInputStream(byte[] data) {
      super(data);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private byte[] decompress(byte[] compressed) throws IOException {
    try (InputStream in = compressor.decompressStream(new ByteArrayInputStream(compressed))) {
      return IOUtils.toByteArray(in);
    }
  }

  private byte[] randomBytes(int size) {
    byte[] data = new byte[size];
    new Random(42).nextBytes(data);
    return data;
  }

  private byte[] readAllWithBufferSize(InputStream in, int bufferSize) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[bufferSize];
    int n;
    while ((n = in.read(buffer, 0, buffer.length)) != -1) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private void assertRoundTrip(byte[] original) throws IOException {
    try (CompressingInputStream in =
        new CompressingInputStream(new ByteArrayInputStream(original), compressor)) {
      byte[] compressed = IOUtils.toByteArray(in);
      assertArrayEquals(original, decompress(compressed));
      assertEquals(compressed.length, in.getCompressedBytes());
    }
  }

  @Test
  public void testRoundTrip_empty() throws IOException {
    assertRoundTrip(new byte[0]);
  }

  @Test
  public void testRoundTrip_small() throws IOException {
    assertRoundTrip("hello world, this is some test data to compress".getBytes());
  }

  @Test
  public void testRoundTrip_largeCompressible() throws IOException {
    // 10 MB of zeros compresses to almost nothing, and LZ4 only emits output once a block (4 MB
    // by default) fills, so many source chunks must be consumed before any output is available.
    assertRoundTrip(new byte[10 * 1024 * 1024]);
  }

  @Test
  public void testRoundTrip_largeIncompressible() throws IOException {
    assertRoundTrip(randomBytes(6 * 1024 * 1024));
  }

  @Test
  public void testRoundTrip_variousReadSizes() throws IOException {
    byte[] original = randomBytes(300 * 1024);
    for (int bufferSize : new int[] {1, 7, 8192, 1024 * 1024}) {
      try (CompressingInputStream in =
          new CompressingInputStream(new ByteArrayInputStream(original), compressor)) {
        byte[] compressed = readAllWithBufferSize(in, bufferSize);
        assertArrayEquals("buffer size " + bufferSize, original, decompress(compressed));
      }
    }
  }

  @Test
  public void testRoundTrip_singleByteReads() throws IOException {
    byte[] original = randomBytes(20 * 1024);
    try (CompressingInputStream in =
        new CompressingInputStream(new ByteArrayInputStream(original), compressor)) {
      ByteArrayOutputStream compressed = new ByteArrayOutputStream();
      int b;
      while ((b = in.read()) != -1) {
        compressed.write(b);
      }
      assertArrayEquals(original, decompress(compressed.toByteArray()));
      assertEquals(compressed.size(), in.getCompressedBytes());
    }
  }

  @Test
  public void testReadAfterEndReturnsMinusOne() throws IOException {
    try (CompressingInputStream in =
        new CompressingInputStream(new ByteArrayInputStream(new byte[100]), compressor)) {
      IOUtils.toByteArray(in);
      assertEquals(-1, in.read());
      assertEquals(-1, in.read(new byte[8], 0, 8));
    }
  }

  @Test
  public void testZeroLengthReadReturnsZero() throws IOException {
    try (CompressingInputStream in =
        new CompressingInputStream(new ByteArrayInputStream(new byte[100]), compressor)) {
      assertEquals(0, in.read(new byte[8], 0, 0));
    }
  }

  @Test
  public void testCloseClosesSource() throws IOException {
    TrackingInputStream source = new TrackingInputStream(randomBytes(1024));
    CompressingInputStream in = new CompressingInputStream(source, compressor);
    in.close();
    assertTrue(source.closed);
  }

  @Test
  public void testCloseIsIdempotent() throws IOException {
    TrackingInputStream source = new TrackingInputStream(randomBytes(1024));
    CompressingInputStream in = new CompressingInputStream(source, compressor);
    in.close();
    in.close();
  }

  @Test
  public void testReadAfterCloseThrows() throws IOException {
    CompressingInputStream in =
        new CompressingInputStream(new ByteArrayInputStream(new byte[10]), compressor);
    in.close();
    try {
      in.read();
      fail("Expected IOException reading a closed stream");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testCloseBeforeFullyReadClosesSource() throws IOException {
    TrackingInputStream source = new TrackingInputStream(randomBytes(5 * 1024 * 1024));
    CompressingInputStream in = new CompressingInputStream(source, compressor);
    in.read(new byte[16]);
    in.close();
    assertTrue(source.closed);
  }

  @Test
  public void testSourceFailurePropagates() throws IOException {
    InputStream failing =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("boom");
          }
        };
    try (CompressingInputStream in = new CompressingInputStream(failing, compressor)) {
      IOUtils.toByteArray(in);
      fail("Expected IOException from failing source");
    } catch (IOException e) {
      assertEquals("boom", e.getMessage());
    }
  }
}
