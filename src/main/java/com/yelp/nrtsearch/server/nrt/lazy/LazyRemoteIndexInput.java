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

import java.io.EOFException;
import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * {@link IndexInput} for a file that stays in the remote backend. Reads are served from fixed size
 * blocks, fetched with byte range reads and kept in a {@link RemoteBlockCache}. Small reads go
 * through a {@value #BUFFER_SIZE} byte buffer, so reading single bytes and numbers doesn't go to
 * the cache for each value.
 *
 * <p>The header and footer are served from the stub, without a remote read, so that opening the
 * file and checking its identity (as replicas do for every file of a new index version) is cheap.
 */
public class LazyRemoteIndexInput extends IndexInput implements RandomAccessInput {
  static final int BUFFER_SIZE = 4096;

  /** Reads a byte range of the remote file. */
  @FunctionalInterface
  public interface RangeReader {
    byte[] read(long offset, int length) throws IOException;
  }

  private final String fileId;
  private final RangeReader reader;
  private final RemoteBlockCache cache;
  private final byte[] header;
  private final byte[] footer;
  private final long fileLength;
  private final long footerStart;
  // offset and length of this input within the file, which differ from (0, fileLength) for slices
  private final long sliceOffset;
  private final long length;

  // position within this input
  private long pos;
  // buffered bytes of the file [bufferStart, bufferStart + bufferLength), never header or footer
  private byte[] buffer;
  private long bufferStart;
  private int bufferLength;

  public LazyRemoteIndexInput(
      String resourceDescription,
      String fileId,
      long fileLength,
      byte[] header,
      byte[] footer,
      RangeReader reader,
      RemoteBlockCache cache) {
    this(resourceDescription, fileId, fileLength, header, footer, reader, cache, 0, fileLength);
  }

  private LazyRemoteIndexInput(
      String resourceDescription,
      String fileId,
      long fileLength,
      byte[] header,
      byte[] footer,
      RangeReader reader,
      RemoteBlockCache cache,
      long sliceOffset,
      long length) {
    super(resourceDescription);
    this.fileId = fileId;
    this.fileLength = fileLength;
    this.header = header == null ? new byte[0] : header;
    this.footer = footer == null ? new byte[0] : footer;
    this.footerStart = fileLength - this.footer.length;
    this.reader = reader;
    this.cache = cache;
    this.sliceOffset = sliceOffset;
    this.length = length;
  }

  // -------------------------------------------------------------------------------------------
  // Reading at an absolute file position
  // -------------------------------------------------------------------------------------------

  private byte byteAt(long filePos) throws IOException {
    if (filePos < header.length) {
      return header[(int) filePos];
    }
    if (filePos >= footerStart) {
      return footer[(int) (filePos - footerStart)];
    }
    if (filePos < bufferStart || filePos >= bufferStart + bufferLength) {
      fillBuffer(filePos);
    }
    return buffer[(int) (filePos - bufferStart)];
  }

  /** Buffer bytes from filePos, up to the end of its block or the footer. */
  private void fillBuffer(long filePos) throws IOException {
    if (buffer == null) {
      buffer = new byte[BUFFER_SIZE];
    }
    int blockSize = cache.getBlockSize();
    long block = filePos / blockSize;
    long blockEnd = Math.min((block + 1) * blockSize, footerStart);
    int n = (int) Math.min(BUFFER_SIZE, blockEnd - filePos);
    readFromCache(filePos, buffer, 0, n);
    bufferStart = filePos;
    bufferLength = n;
  }

  /** Copy bytes from one block of the file, which must not include the header or footer. */
  private void readFromCache(long filePos, byte[] dest, int destOffset, int len)
      throws IOException {
    int blockSize = cache.getBlockSize();
    long block = filePos / blockSize;
    long blockStart = block * blockSize;
    int blockLength = (int) Math.min(blockSize, fileLength - blockStart);
    cache.read(
        fileId,
        block,
        (int) (filePos - blockStart),
        dest,
        destOffset,
        len,
        () -> reader.read(blockStart, blockLength));
  }

  /**
   * Copy bytes from an absolute file position, returning how many were copied (from one region).
   */
  private int copyAt(long filePos, byte[] b, int offset, int len) throws IOException {
    if (filePos < header.length) {
      int n = (int) Math.min(len, header.length - filePos);
      System.arraycopy(header, (int) filePos, b, offset, n);
      return n;
    }
    if (filePos >= footerStart) {
      int n = (int) Math.min(len, fileLength - filePos);
      System.arraycopy(footer, (int) (filePos - footerStart), b, offset, n);
      return n;
    }
    if (filePos >= bufferStart && filePos < bufferStart + bufferLength) {
      int n = (int) Math.min(len, bufferStart + bufferLength - filePos);
      System.arraycopy(buffer, (int) (filePos - bufferStart), b, offset, n);
      return n;
    }
    int blockSize = cache.getBlockSize();
    long blockEnd = Math.min((filePos / blockSize + 1) * blockSize, footerStart);
    // stop at the end of the block, and at the footer (served from the stub)
    int n = (int) Math.min(len, blockEnd - filePos);
    if (n < BUFFER_SIZE) {
      // small read, buffer it for the reads that follow
      fillBuffer(filePos);
      n = Math.min(n, bufferLength);
      System.arraycopy(buffer, 0, b, offset, n);
    } else {
      readFromCache(filePos, b, offset, n);
    }
    return n;
  }

  private void readFully(long inputPos, byte[] b, int offset, int len) throws IOException {
    if (inputPos < 0 || len < 0 || inputPos + len > length) {
      throw new EOFException(
          "read past EOF: pos=" + inputPos + " len=" + len + " length=" + length + ": " + this);
    }
    long filePos = sliceOffset + inputPos;
    while (len > 0) {
      int n = copyAt(filePos, b, offset, len);
      filePos += n;
      offset += n;
      len -= n;
    }
  }

  // -------------------------------------------------------------------------------------------
  // IndexInput
  // -------------------------------------------------------------------------------------------

  @Override
  public byte readByte() throws IOException {
    if (pos >= length) {
      throw new EOFException("read past EOF: " + this);
    }
    return byteAt(sliceOffset + pos++);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    readFully(pos, b, offset, len);
    pos += len;
  }

  @Override
  public long getFilePointer() {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    if (newPos < 0 || newPos > length) {
      throw new EOFException("seek past EOF: pos=" + newPos + " length=" + length + ": " + this);
    }
    pos = newPos;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public void close() {}

  @Override
  public LazyRemoteIndexInput slice(String sliceDescription, long offset, long sliceLength)
      throws IOException {
    if (offset < 0 || sliceLength < 0 || offset + sliceLength > length) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + ",length="
              + sliceLength
              + ",fileLength="
              + length
              + ": "
              + this);
    }
    return new LazyRemoteIndexInput(
        getFullSliceDescription(sliceDescription),
        fileId,
        fileLength,
        header,
        footer,
        reader,
        cache,
        sliceOffset + offset,
        sliceLength);
  }

  @Override
  public LazyRemoteIndexInput clone() {
    LazyRemoteIndexInput clone = (LazyRemoteIndexInput) super.clone();
    // clones are used by other threads, give the clone its own buffer
    clone.buffer = null;
    clone.bufferLength = 0;
    return clone;
  }

  // -------------------------------------------------------------------------------------------
  // RandomAccessInput
  // -------------------------------------------------------------------------------------------

  @Override
  public byte readByte(long position) throws IOException {
    if (position < 0 || position >= length) {
      throw new EOFException("read past EOF: pos=" + position + ": " + this);
    }
    return byteAt(sliceOffset + position);
  }

  @Override
  public void readBytes(long position, byte[] bytes, int offset, int len) throws IOException {
    readFully(position, bytes, offset, len);
  }

  @Override
  public short readShort(long position) throws IOException {
    byte[] b = new byte[Short.BYTES];
    readFully(position, b, 0, b.length);
    return (short) ((b[0] & 0xFF) | (b[1] & 0xFF) << 8);
  }

  @Override
  public int readInt(long position) throws IOException {
    byte[] b = new byte[Integer.BYTES];
    readFully(position, b, 0, b.length);
    return (b[0] & 0xFF) | (b[1] & 0xFF) << 8 | (b[2] & 0xFF) << 16 | (b[3] & 0xFF) << 24;
  }

  @Override
  public long readLong(long position) throws IOException {
    return (readInt(position) & 0xFFFFFFFFL) | ((long) readInt(position + 4)) << 32;
  }
}
