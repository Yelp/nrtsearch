/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.backup;

import static com.amazonaws.services.s3.internal.Constants.MAXIMUM_UPLOAD_PARTS;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Output stream implementation to upload a tar file output to s3. Uses the uncompressed total file
 * size to determine the part size for multi-part upload. Parts are uploaded in parallel using the
 * given {@link ThreadPoolExecutor}.
 *
 * <p>Written data is buffered into byte arrays. One array is created per potential thread pool
 * thread, and recycled using a buffer pool. The amount of heap used is therefore num_threads *
 * part_size. Since the pool size is 20 and the minimum part size is 5MB, this will use up to 100MB
 * of heap for file sizes up to ~50GB and increase linearly thereafter.
 *
 * <p>Note that part sizes above the max signed integer are not supported, as a single byte buffer
 * is used. However, there would likely be heap pressure/OOM issues long before that.
 */
public class TarUploadOutputStream extends OutputStream {
  private static final long MIN_PART_SIZE = 5 * 1024 * 1024;
  private static final long RESERVED_PARTS = 3;
  private static final long STATUS_INTERVAL_MS = 5000;
  private static final Logger logger = LoggerFactory.getLogger(TarUploadOutputStream.class);

  private final String bucketName;
  private final String key;
  private final AmazonS3 s3Client;
  private final ThreadPoolExecutor executor;
  private final int partSize;
  private final String uploadId;
  private final List<PartETag> tagList = new ArrayList<>();
  private final LinkedList<Future<UploadJob>> queuedJobs = new LinkedList<>();
  private final LinkedList<byte[]> bufferPool = new LinkedList<>();

  private byte[] currentBuffer;
  private int currentOffset;
  private int partNum;

  private long lastStatusTimeMs = System.currentTimeMillis();
  private long uploadedBytes = 0;

  private boolean closed = false;
  private boolean canceled = false;

  /**
   * Constructor.
   *
   * @param bucketName s3 bucket name
   * @param key s3 file key
   * @param uncompressedSize total size of all files in tar
   * @param s3Client s3 client
   * @param executor thread pool for parallel part uploading
   */
  TarUploadOutputStream(
      String bucketName,
      String key,
      long uncompressedSize,
      AmazonS3 s3Client,
      ThreadPoolExecutor executor) {
    this.bucketName = bucketName;
    this.key = key;
    this.s3Client = s3Client;
    this.executor = executor;

    // Since we are compressing, the resulting data should be smaller. However, it is good to
    // reserve a few extra parts in case that assumption is not true for some reason.
    double optimalPartSize =
        (double) uncompressedSize / (double) (MAXIMUM_UPLOAD_PARTS - RESERVED_PARTS);
    // round up so we don't push the upload over the maximum number of parts
    optimalPartSize = Math.ceil(optimalPartSize);
    long fullPartSize = (long) Math.max(optimalPartSize, MIN_PART_SIZE);
    // This won't be the case unless the file is ~20TB
    if (fullPartSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Upload part size is too large: " + fullPartSize);
    }

    partSize = (int) fullPartSize;
    currentBuffer = new byte[partSize];
    currentOffset = 0;
    partNum = 1;

    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(bucketName, key);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    uploadId = initResponse.getUploadId();
  }

  @Override
  public void write(int b) throws IOException {
    currentBuffer[currentOffset] = (byte) b;
    currentOffset++;
    if (currentOffset == currentBuffer.length) {
      startPartCopy(false);
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    int remaining = len;
    int sourceOffset = off;
    while (remaining > 0) {
      int thisCopySize = Math.min(remaining, currentBuffer.length - currentOffset);
      System.arraycopy(b, sourceOffset, currentBuffer, currentOffset, thisCopySize);
      remaining -= thisCopySize;
      sourceOffset += thisCopySize;
      currentOffset += thisCopySize;
      if (currentOffset == currentBuffer.length) {
        startPartCopy(false);
      }
    }
  }

  private void startPartCopy(boolean lastPart) throws IOException {
    UploadJob uploadJob = new UploadJob(currentBuffer, currentOffset, partNum);
    queuedJobs.add(executor.submit(uploadJob));

    // Clear any completed tasks
    for (Iterator<Future<UploadJob>> iterator = queuedJobs.iterator(); iterator.hasNext(); ) {
      Future<UploadJob> future = iterator.next();
      if (future.isDone()) {
        try {
          UploadJob finishedJob = future.get();
          tagList.add(finishedJob.tag);
          uploadedBytes += finishedJob.length;
        } catch (Exception e) {
          throw new IOException("Part copy error", e);
        }
        iterator.remove();
      }
    }

    // Periodically log progress
    long currentTimeMs = System.currentTimeMillis();
    if (currentTimeMs - lastStatusTimeMs > STATUS_INTERVAL_MS) {
      logger.info(String.format("Upload status: %d", uploadedBytes));
      lastStatusTimeMs = currentTimeMs;
    }

    if (!lastPart) {
      // Only create one buffer per potential thread
      if (partNum < executor.getMaximumPoolSize()) {
        currentBuffer = new byte[partSize];
      } else {
        synchronized (bufferPool) {
          while (bufferPool.isEmpty()) {
            try {
              bufferPool.wait();
            } catch (Exception e) {
              throw new IOException("Error waiting for buffer", e);
            }
          }
          currentBuffer = bufferPool.pollFirst();
        }
      }
      currentOffset = 0;
      partNum++;
    }
  }

  /** Cancel the active multi-part upload to s3. */
  public void cancel() {
    canceled = true;
    AbortMultipartUploadRequest abortRequest =
        new AbortMultipartUploadRequest(bucketName, key, uploadId);
    s3Client.abortMultipartUpload(abortRequest);
  }

  /**
   * Complete the active multi-part upload to s3.
   *
   * @throws IOException on error uploading file parts
   * @throws IllegalStateException if the stream has not been closed yet, or if the stream has
   *     already been canceled
   */
  public void complete() throws IOException {
    if (!closed) {
      throw new IllegalStateException("Stream is not closed");
    }
    if (canceled) {
      throw new IllegalStateException("Trying to complete canceled stream");
    }

    while (!queuedJobs.isEmpty()) {
      try {
        UploadJob finishedJob = queuedJobs.pollFirst().get();
        tagList.add(finishedJob.tag);
        uploadedBytes += finishedJob.length;
      } catch (Exception e) {
        throw new IOException("Part copy error", e);
      }
    }
    CompleteMultipartUploadRequest compRequest =
        new CompleteMultipartUploadRequest(bucketName, key, uploadId, tagList);
    s3Client.completeMultipartUpload(compRequest);
    logger.info("Upload complete, total size: " + uploadedBytes);
  }

  @Override
  public void close() throws IOException {
    // flush the last part
    if (currentOffset > 0) {
      startPartCopy(true);
    }
    closed = true;
  }

  /** Task to upload a file part to s3. */
  private class UploadJob implements Callable<UploadJob> {
    private final byte[] buffer;
    private final int length;
    private final int part;

    private PartETag tag;

    /**
     * Constructor.
     *
     * @param buffer part data
     * @param length length of part data
     * @param part part number
     */
    UploadJob(byte[] buffer, int length, int part) {
      this.buffer = buffer;
      this.length = length;
      this.part = part;
    }

    @Override
    public UploadJob call() throws Exception {
      try {
        UploadPartRequest uploadRequest =
            new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(part)
                .withPartSize(length)
                .withInputStream(new ByteArrayInputStream(buffer, 0, length));

        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        tag = uploadResult.getPartETag();
        return this;
      } finally {
        // add my buffer back to pool for reuse
        synchronized (bufferPool) {
          bufferPool.add(buffer);
          bufferPool.notifyAll();
        }
      }
    }
  }
}
