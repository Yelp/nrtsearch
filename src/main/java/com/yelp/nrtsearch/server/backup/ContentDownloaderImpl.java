/*
 * Copyright 2021 Yelp Inc.
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

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.google.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentDownloaderImpl implements ContentDownloader {
  private static final String TMP_SUFFIX = ".tmp";
  private static final int NUM_S3_THREADS = 20;
  private final Tar tar;
  private final boolean downloadAsStream;
  private final TransferManager transferManager;
  private final String bucketName;
  private static final Logger logger = LoggerFactory.getLogger(ContentDownloaderImpl.class);
  private final ThreadPoolExecutor executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);

  @Inject
  public ContentDownloaderImpl(
      Tar tar, TransferManager transferManager, String bucketName, boolean downloadAsStream) {
    this.tar = tar;
    this.transferManager = transferManager;
    this.bucketName = bucketName;
    this.downloadAsStream = downloadAsStream;
  }

  @Override
  public void getVersionContent(
      final String serviceName, final String resource, final String hash, final Path destDirectory)
      throws IOException {
    final String absoluteResourcePath = String.format("%s/%s/%s", serviceName, resource, hash);
    final Path parentDirectory = destDirectory.getParent();
    final Path tmpFile = parentDirectory.resolve(getTmpName());

    final InputStream s3InputStream;
    if (downloadAsStream) {
      // Stream the file download from s3 instead of writing to a file first
      GetObjectMetadataRequest metadataRequest =
          new GetObjectMetadataRequest(bucketName, absoluteResourcePath);
      ObjectMetadata fullMetadata =
          transferManager.getAmazonS3Client().getObjectMetadata(metadataRequest);
      logger.debug("Full object size: " + fullMetadata.getContentLength());

      // get metadata for the 1st file part, needed to find the total number of parts
      ObjectMetadata partMetadata =
          transferManager.getAmazonS3Client().getObjectMetadata(metadataRequest.withPartNumber(1));
      int numParts = partMetadata.getPartCount() != null ? partMetadata.getPartCount() : 1;
      logger.debug("Object parts: " + numParts);
      s3InputStream = getObjectStream(absoluteResourcePath, numParts);
      logger.debug("Object streaming started...");
    } else {
      Download download =
          transferManager.download(
              new GetObjectRequest(bucketName, absoluteResourcePath),
              tmpFile.toFile(),
              new ContentDownloaderImpl.S3ProgressListenerImpl(serviceName, resource, "download"));
      try {
        download.waitForCompletion();
        logger.debug("S3 Download complete");
      } catch (InterruptedException e) {
        throw new IOException("S3 Download failed", e);
      }

      s3InputStream = new FileInputStream(tmpFile.toFile());
    }

    wrapInputStream(destDirectory, parentDirectory, tmpFile, s3InputStream, hash);
  }

  private void wrapInputStream(
      Path destDirectory,
      Path parentDirectory,
      Path tmpFile,
      InputStream s3InputStream,
      String hash)
      throws IOException {
    final InputStream compressorInputStream;
    if (tar.getCompressionMode().equals(Tar.CompressionMode.LZ4)) {
      compressorInputStream = new LZ4FrameInputStream(s3InputStream);
    } else if (tar.getCompressionMode().equals(Tar.CompressionMode.GZIP)) {
      compressorInputStream = new GzipCompressorInputStream(s3InputStream, true);
    } else {
      compressorInputStream = s3InputStream;
    }
    try (final TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(compressorInputStream); ) {
      extractContent(
          destDirectory, parentDirectory, tmpFile, tarArchiveInputStream, s3InputStream, hash);
    }
  }

  private void extractContent(
      Path destDirectory,
      Path parentDirectory,
      Path tmpFile,
      TarArchiveInputStream tarArchiveInputStream,
      InputStream s3InputStream,
      String hash)
      throws IOException {
    if (Files.exists(destDirectory)) {
      logger.info("Directory {} already exists, not re-downloading from Archiver", destDirectory);
      return;
    }
    final Path tmpDirectory = parentDirectory.resolve(getTmpName());
    try {
      long tarBefore = System.nanoTime();
      logger.debug("Extract tar started...");
      if (tar instanceof TarImpl) {
        tar.extractTar(tarArchiveInputStream, tmpDirectory);
      } else if (tar instanceof NoTarImpl) {
        tar.extractTar(s3InputStream, tmpDirectory, hash);
      } else {
        throw new RuntimeException("Invalid Tar instance initialized");
      }
      long tarAfter = System.nanoTime();
      logger.debug(
          "Extract tar time " + (tarAfter - tarBefore) / (1000 * 1000 * 1000) + " seconds");
      Files.move(tmpDirectory, destDirectory);
    } finally {
      if (Files.exists(tmpDirectory)) {
        FileUtils.deleteDirectory(tmpDirectory.toFile());
      }
      if (Files.exists(tmpFile)) {
        Files.delete(tmpFile);
      }
    }
  }

  @Override
  public AmazonS3 getS3Client() {
    return transferManager.getAmazonS3Client();
  }

  @Override
  public String getBucketName() {
    return bucketName;
  }

  @Override
  public boolean downloadAsStream() {
    return downloadAsStream;
  }

  private InputStream getObjectStream(String key, int numParts) {
    // enumerate the individual part streams and return a combined view
    Enumeration<InputStream> objectEnum = getObjectEnum(key, numParts);
    return new SequenceInputStream(objectEnum);
  }

  private Enumeration<InputStream> getObjectEnum(String key, int numParts) {
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
                    S3Object s3Object = transferManager.getAmazonS3Client().getObject(getRequest);
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

  private String getTmpName() {
    return UUID.randomUUID().toString() + TMP_SUFFIX;
  }

  public static class S3ProgressListenerImpl implements S3ProgressListener {
    private static final Logger logger =
        LoggerFactory.getLogger(ContentDownloaderImpl.S3ProgressListenerImpl.class);

    private static final long LOG_THRESHOLD_BYTES = 1024 * 1024 * 500; // 500 MB
    private static final long LOG_THRESHOLD_SECONDS = 30;

    private final String serviceName;
    private final String resource;
    private final String operation;

    private final Semaphore lock = new Semaphore(1);
    private final AtomicLong totalBytesTransferred = new AtomicLong();
    private long bytesTransferredSinceLastLog = 0;
    private LocalDateTime lastLoggedTime = LocalDateTime.now();

    public S3ProgressListenerImpl(String serviceName, String resource, String operation) {
      this.serviceName = serviceName;
      this.resource = resource;
      this.operation = operation;
    }

    @Override
    public void onPersistableTransfer(PersistableTransfer persistableTransfer) {}

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      long totalBytes = totalBytesTransferred.addAndGet(progressEvent.getBytesTransferred());

      boolean acquired = lock.tryAcquire();

      if (acquired) {
        try {
          bytesTransferredSinceLastLog += progressEvent.getBytesTransferred();
          long secondsSinceLastLog =
              Duration.between(lastLoggedTime, LocalDateTime.now()).getSeconds();

          if (bytesTransferredSinceLastLog > LOG_THRESHOLD_BYTES
              || secondsSinceLastLog > LOG_THRESHOLD_SECONDS) {
            logger.info(
                String.format(
                    "service: %s, resource: %s, %s transferred bytes: %s",
                    serviceName, resource, operation, totalBytes));
            bytesTransferredSinceLastLog = 0;
            lastLoggedTime = LocalDateTime.now();
          }
        } finally {
          lock.release();
        }
      }
    }
  }
}
