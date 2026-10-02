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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An {@link InputStream} that yields the compressed form of a source stream, compressing lazily as
 * it is read. Only a small buffer of compressed output is held, so memory use is bounded by the
 * compressor's internal block size plus one source chunk, regardless of source size.
 *
 * <p>Compression happens on the reading thread, so there is no producer thread that can be left
 * blocked if the consumer stops reading. Not thread safe.
 */
public class CompressingInputStream extends InputStream {
  private static final int SOURCE_CHUNK_SIZE = 64 * 1024;

  /** Output buffer that exposes its backing array so reads can copy from it directly. */
  private static final class Buffer extends ByteArrayOutputStream {
    byte[] array() {
      return buf;
    }
  }

  private final InputStream source;
  private final Buffer buffer = new Buffer();
  private final OutputStream compressor;
  private final byte[] chunk = new byte[SOURCE_CHUNK_SIZE];
  private int bufferPos = 0;
  private boolean sourceDone = false;
  private boolean closed = false;
  private long compressedBytes = 0;

  /**
   * Constructor.
   *
   * @param source stream of uncompressed data, closed when this stream is closed
   * @param fileCompressor compressor used to produce the output
   * @throws IOException on error creating the compressing stream
   */
  public CompressingInputStream(InputStream source, FileCompressor fileCompressor)
      throws IOException {
    this.source = source;
    OutputStream compressingStream;
    try {
      compressingStream = fileCompressor.compressStream(buffer);
    } catch (IOException | RuntimeException e) {
      try {
        source.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
    this.compressor = compressingStream;
  }

  /** Get the number of compressed bytes that have been read from this stream so far. */
  public long getCompressedBytes() {
    return compressedBytes;
  }

  /**
   * Ensure there is unread compressed data in the buffer, pulling and compressing source data as
   * needed.
   *
   * @return false if the source is exhausted and all compressed data has been read
   */
  private boolean fill() throws IOException {
    while (bufferPos >= buffer.size()) {
      buffer.reset();
      bufferPos = 0;
      if (sourceDone) {
        return false;
      }
      int read = source.read(chunk);
      if (read < 0) {
        sourceDone = true;
        // closing the compressor flushes the remaining block and the end-of-stream marker
        compressor.close();
      } else if (read > 0) {
        compressor.write(chunk, 0, read);
      }
    }
    return true;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  @Override
  public int read() throws IOException {
    ensureOpen();
    if (!fill()) {
      return -1;
    }
    compressedBytes++;
    return buffer.array()[bufferPos++] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    ensureOpen();
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) {
      return 0;
    }
    if (!fill()) {
      return -1;
    }
    int count = Math.min(len, buffer.size() - bufferPos);
    System.arraycopy(buffer.array(), bufferPos, b, off, count);
    bufferPos += count;
    compressedBytes += count;
    return count;
  }

  @Override
  public int available() throws IOException {
    ensureOpen();
    return buffer.size() - bufferPos;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      if (!sourceDone) {
        // release any resources held by the compressor, even when abandoned before the end
        sourceDone = true;
        compressor.close();
      }
    } catch (IOException e) {
      failure = e;
    }
    try {
      source.close();
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
