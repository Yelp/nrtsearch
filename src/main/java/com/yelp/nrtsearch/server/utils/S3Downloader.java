/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.Closeable;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Downloader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(S3Downloader.class);

  public static final int NUM_S3_THREADS = 20;

  private final ThreadPoolExecutor executor;
  private final AmazonS3 s3;

  public S3Downloader(AmazonS3 s3) {
    this.s3 = s3;
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);
  }

  public S3Downloader(AmazonS3 s3, int numS3Threads) {
    this.s3 = s3;
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numS3Threads);
  }

  public S3Downloader(AmazonS3 s3, ThreadPoolExecutor executor) {
    this.s3 = s3;
    this.executor = executor;
  }

  /**
   * Download a file from the specified bucket and key.
   *
   * @param s3Path Complete S3 path of the file
   * @return {@link InputStream} of the file being streamed
   * @throws IllegalArgumentException if bucket or path not found
   */
  public InputStream downloadFromS3Path(String s3Path) {
    AmazonS3URI s3URI = new AmazonS3URI(s3Path);
    return downloadFromS3Path(s3URI.getBucket(), s3URI.getKey());
  }

  /**
   * Download a file from the specified bucket and key.
   *
   * @param bucketName Bucket name in S3
   * @param absoluteResourcePath Key for the file in the bucket
   * @return {@link InputStream} of the file being streamed
   * @throws IllegalArgumentException if bucket or path not found
   */
  public InputStream downloadFromS3Path(String bucketName, String absoluteResourcePath) {
    logger.info("Downloading {} from bucket {}", absoluteResourcePath, bucketName);
    final InputStream s3InputStream;
    // Stream the file download from s3 instead of writing to a file first
    GetObjectMetadataRequest metadataRequest =
        new GetObjectMetadataRequest(bucketName, absoluteResourcePath);
    try {
      ObjectMetadata fullMetadata = s3.getObjectMetadata(metadataRequest);
      logger.info("Full object size: " + fullMetadata.getContentLength());
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        String error =
            String.format("Object s3://%s/%s not found", bucketName, absoluteResourcePath);
        throw new IllegalArgumentException(error, e);
      }
      throw e;
    }

    // get metadata for the 1st file part, needed to find the total number of parts
    ObjectMetadata partMetadata = s3.getObjectMetadata(metadataRequest.withPartNumber(1));
    int numParts = partMetadata.getPartCount() != null ? partMetadata.getPartCount() : 1;
    logger.info("Object parts: " + numParts);
    s3InputStream = getObjectStream(bucketName, absoluteResourcePath, numParts);
    logger.info("Object streaming started...");
    return s3InputStream;
  }

  private InputStream getObjectStream(String bucketName, String key, int numParts) {
    // enumerate the individual part streams and return a combined view
    Enumeration<InputStream> objectEnum = getObjectEnum(bucketName, key, numParts);
    return new SequenceInputStream(objectEnum);
  }

  private Enumeration<InputStream> getObjectEnum(String bucketName, String key, int numParts) {
    return new Enumeration<>() {
      final long STATUS_INTERVAL_MS = 5000;
      long lastStatusTimeMs = System.currentTimeMillis();
      final LinkedList<Future<InputStream>> pendingParts = new LinkedList<>();
      int currentPart = 1;
      int queuedPart = 1;

      @Override
      public boolean hasMoreElements() {
        return currentPart <= numParts;
      }

      @Override
      public InputStream nextElement() {
        // top off the work queue so parts can download in parallel
        while (pendingParts.size() < NUM_S3_THREADS && queuedPart <= numParts) {
          // set to final variable for use in lambda
          final int finalPart = queuedPart;
          pendingParts.add(
              executor.submit(
                  () -> {
                    GetObjectRequest getRequest =
                        new GetObjectRequest(bucketName, key).withPartNumber(finalPart);
                    S3Object s3Object = s3.getObject(getRequest);
                    return s3Object.getObjectContent();
                  }));
          queuedPart++;
        }

        // Periodically log progress
        long currentTimeMs = System.currentTimeMillis();
        if (currentTimeMs - lastStatusTimeMs > STATUS_INTERVAL_MS) {
          double percentCompleted = 100.0 * (currentPart - 1) / ((double) numParts);
          logger.info(String.format("Download status: %.2f%%", percentCompleted));
          lastStatusTimeMs = currentTimeMs;
        }

        currentPart++;

        // return stream for next part from fifo future queue
        try {
          return pendingParts.pollFirst().get();
        } catch (Exception e) {
          throw new RuntimeException("Error downloading file part", e);
        }
      }
    };
  }

  @Override
  public void close() {
    try {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
