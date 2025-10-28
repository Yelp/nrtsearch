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
package com.yelp.nrtsearch.server.index;

import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;

/**
 * An {@link org.apache.lucene.store.IndexInput} implementation that reads index files directly from
 * S3. This implementation supports random access via S3 range requests and buffers reads to
 * minimize S3 API calls.
 *
 * <p>This class uses S3 range requests (HTTP Range header) to support Lucene's random access
 * patterns efficiently.
 */
public class S3IndexInput extends BufferedIndexInput {
  private static final int DEFAULT_BUFFER_SIZE = 16384; // 16KB buffer

  private final S3Backend s3Backend;
  private final String service;
  private final String indexIdentifier;
  private final String fileName;
  private final NrtFileMetaData fileMetaData;
  private final long fileLength;

  private long filePointer = 0;
  private boolean isClone = false;

  /**
   * Constructor for S3IndexInput.
   *
   * @param resourceDescription resource description for debugging
   * @param s3Backend S3 backend for downloading files
   * @param service service name
   * @param indexIdentifier index identifier
   * @param fileName file name
   * @param fileMetaData file metadata
   * @param context IO context
   */
  public S3IndexInput(
      String resourceDescription,
      S3Backend s3Backend,
      String service,
      String indexIdentifier,
      String fileName,
      NrtFileMetaData fileMetaData,
      IOContext context) {
    super(resourceDescription, determineBufferSize(context));
    this.s3Backend = s3Backend;
    this.service = service;
    this.indexIdentifier = indexIdentifier;
    this.fileName = fileName;
    this.fileMetaData = fileMetaData;
    this.fileLength = fileMetaData.length;
  }

  /**
   * Private constructor for cloning.
   *
   * @param other the S3IndexInput to clone
   */
  private S3IndexInput(S3IndexInput other) {
    super(other.toString(), other.getBufferSize());
    this.s3Backend = other.s3Backend;
    this.service = other.service;
    this.indexIdentifier = other.indexIdentifier;
    this.fileName = other.fileName;
    this.fileMetaData = other.fileMetaData;
    this.fileLength = other.fileLength;
    this.filePointer = other.filePointer;
    this.isClone = true;
  }

  /**
   * Determine appropriate buffer size based on IOContext.
   *
   * @param context IO context
   * @return buffer size in bytes
   */
  private static int determineBufferSize(IOContext context) {
    if (context == null) {
      return DEFAULT_BUFFER_SIZE;
    }

    // Use larger buffers for merging and flushing operations
    return switch (context.context()) {
      case MERGE -> 65536; // 64KB for merges
      case FLUSH -> 32768; // 32KB for flushes
      case DEFAULT -> 16384; // 16KB for default reads
    };
  }

  @Override
  protected void readInternal(ByteBuffer b) throws IOException {
    long position = getFilePointer();
    int length = b.remaining();

    if (position + length > fileLength) {
      throw new EOFException(
          "Read past EOF: position="
              + position
              + " length="
              + length
              + " fileLength="
              + fileLength
              + " file="
              + fileName);
    }

    // Download the required range from S3
    long startTime = System.nanoTime();
    try (InputStream inputStream =
        s3Backend.downloadIndexFileRange(
            service, indexIdentifier, fileName, fileMetaData, position, length)) {
      byte[] buffer = new byte[8192];
      int totalRead = 0;
      while (totalRead < length) {
        int toRead = Math.min(buffer.length, length - totalRead);
        int read = inputStream.read(buffer, 0, toRead);
        if (read == -1) {
          throw new EOFException(
              "Unexpected EOF from S3: read="
                  + totalRead
                  + " expected="
                  + length
                  + " file="
                  + fileName);
        }
        b.put(buffer, 0, read);
        totalRead += read;
      }

      // Record metrics for successful read
      long endTime = System.nanoTime();
      double latencySeconds = (endTime - startTime) / 1_000_000_000.0;
      com.yelp.nrtsearch.server.monitoring.S3DirectoryMetrics.recordRead(
          getIndexNameFromDescription(toString()), length, latencySeconds);
    } catch (IOException e) {
      // Record error metric
      com.yelp.nrtsearch.server.monitoring.S3DirectoryMetrics.recordError(
          getIndexNameFromDescription(toString()), "IOException");
      throw e;
    } catch (Exception e) {
      // Record error metric for unexpected exceptions
      com.yelp.nrtsearch.server.monitoring.S3DirectoryMetrics.recordError(
          getIndexNameFromDescription(toString()), e.getClass().getSimpleName());
      throw new IOException("Error reading from S3", e);
    }
  }

  /**
   * Extract index name from resource description for metrics. Resource description format is:
   * "S3IndexInput(file=filename, index=indexname)"
   *
   * @param description resource description
   * @return index name, or "unknown" if not found
   */
  private static String getIndexNameFromDescription(String description) {
    int indexStart = description.indexOf("index=");
    if (indexStart == -1) {
      return "unknown";
    }
    int indexEnd = description.indexOf(")", indexStart);
    if (indexEnd == -1) {
      indexEnd = description.length();
    }
    return description.substring(indexStart + 6, indexEnd);
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos > fileLength) {
      throw new EOFException("Seek past EOF: pos=" + pos + " fileLength=" + fileLength);
    }
    this.filePointer = pos;
  }

  @Override
  public void close() throws IOException {
    // Nothing to close - we don't maintain persistent connections
  }

  @Override
  public long length() {
    return fileLength;
  }

  @Override
  public S3IndexInput clone() {
    S3IndexInput clone = (S3IndexInput) super.clone();
    return new S3IndexInput(clone);
  }

  @Override
  public org.apache.lucene.store.IndexInput slice(String sliceDescription, long offset, long length)
      throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.fileLength) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + " length="
              + length
              + " fileLength="
              + this.fileLength);
    }

    // Use BufferedIndexInput's built-in slice implementation
    return super.slice(sliceDescription, offset, length);
  }

  /** Get the file name being read. */
  public String getFileName() {
    return fileName;
  }

  /** Get the file metadata. */
  public NrtFileMetaData getFileMetaData() {
    return fileMetaData;
  }
}
