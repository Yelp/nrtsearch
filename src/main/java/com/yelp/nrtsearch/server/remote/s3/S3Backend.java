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
package com.yelp.nrtsearch.server.remote.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.utils.ZipUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Backend implementation that stored data in amazon s3 object storage. */
public class S3Backend implements RemoteBackend {
  public static final String WARMING_QUERIES_RESOURCE_SUFFIX = "_warming_queries";
  public static final String LATEST_VERSION = "_latest_version";
  static final String VERSION_STRING_FORMAT = "%s/_version/%s/%s";
  static final String RESOURCE_KEY_FORMAT = "%s/%s/%s";
  private static final Logger logger = LoggerFactory.getLogger(S3Backend.class);
  private static final String ZIP_EXTENSION = ".zip";

  public static final int NUM_S3_THREADS = 20;

  private final ThreadPoolExecutor executor;
  private final boolean saveBeforeUnzip;
  private final AmazonS3 s3;
  private final String serviceBucket;
  private final VersionManager versionManger;
  private final TransferManager transferManager;

  /**
   * Constructor.
   *
   * @param configuration configuration
   * @param s3 s3 client
   */
  public S3Backend(LuceneServerConfiguration configuration, AmazonS3 s3) {
    this.s3 = s3;
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);
    this.saveBeforeUnzip = configuration.getSavePluginBeforeUnzip();
    this.serviceBucket = configuration.getBucketName();
    this.versionManger = new VersionManager(s3, serviceBucket);
    this.transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(false)
            .build();
  }

  @Override
  public String downloadPluginIfNeeded(String pluginNameOrPath, Path destPath) {
    if (S3Util.isValidS3FilePath(pluginNameOrPath) && pluginNameOrPath.endsWith(ZIP_EXTENSION)) {
      logger.info("Downloading plugin: {}", pluginNameOrPath);
      ZipUtil.extractZip(downloadFromS3Path(pluginNameOrPath), destPath, saveBeforeUnzip);
      // Assuming that the plugin directory is same as the name of the file
      return S3Util.getS3FileName(pluginNameOrPath).split(".zip")[0];
    }
    return pluginNameOrPath;
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
      if (isNotFoundException(e)) {
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
    transferManager.shutdownNow(false);
    try {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    s3.shutdown();
  }

  @Override
  public boolean exists(String service, String indexIdentifier, IndexResourceType resourceType)
      throws IOException {
    String resource = getResourceName(indexIdentifier, resourceType);
    try {
      getVersionString(service, resource, LATEST_VERSION);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  @Override
  public InputStream downloadStream(
      String service, String indexIdentifier, IndexResourceType resourceType) throws IOException {
    String resource = getResourceName(indexIdentifier, resourceType);
    String latestVersion = getVersionString(service, resource, LATEST_VERSION);
    String versionHash = getVersionString(service, resource, latestVersion);
    String resourceKey = getResourceKey(service, resource, versionHash);
    return downloadFromS3Path(serviceBucket, resourceKey);
  }

  @Override
  public void uploadFile(
      String service, String indexIdentifier, IndexResourceType resourceType, Path file)
      throws IOException {
    if (!Files.exists(file)) {
      throw new IllegalArgumentException("File does not exist: " + file);
    }
    if (!Files.isRegularFile(file)) {
      throw new IllegalArgumentException("Is not regular file: " + file);
    }
    String resource = getResourceName(indexIdentifier, resourceType);
    String versionHash = UUID.randomUUID().toString();
    String resourceKey = getResourceKey(service, resource, versionHash);
    PutObjectRequest request = new PutObjectRequest(serviceBucket, resourceKey, file.toFile());
    request.setGeneralProgressListener(new S3ProgressListenerImpl(service, resource, "upload"));
    Upload upload = transferManager.upload(request);
    try {
      upload.waitForUploadResult();
      logger.info("Upload completed ");
    } catch (InterruptedException e) {
      throw new IOException("Error while uploading to s3. ", e);
    }
    versionManger.blessVersion(service, resource, versionHash);
  }

  @VisibleForTesting
  static String getResourceName(String indexIdentifier, IndexResourceType resourceType) {
    return switch (resourceType) {
      case WARMING_QUERIES -> indexIdentifier + WARMING_QUERIES_RESOURCE_SUFFIX;
    };
  }

  @VisibleForTesting
  static String getResourceKey(String service, String resource, String versionHash) {
    return String.format(RESOURCE_KEY_FORMAT, service, resource, versionHash);
  }

  @VisibleForTesting
  static String getVersionKey(String service, String resource, String version) {
    return String.format(VERSION_STRING_FORMAT, service, resource, version);
  }

  private String getVersionString(
      final String serviceName, final String resource, final String version) throws IOException {
    final String absoluteResourcePath = getVersionKey(serviceName, resource, version);
    try (final S3Object s3Object = s3.getObject(serviceBucket, absoluteResourcePath)) {
      return IOUtils.toString(s3Object.getObjectContent());
    } catch (AmazonS3Exception e) {
      if (isNotFoundException(e)) {
        String error =
            String.format("Object s3://%s/%s not found", serviceBucket, absoluteResourcePath);
        throw new IllegalArgumentException(error, e);
      }
      throw e;
    }
  }

  private boolean isNotFoundException(AmazonS3Exception e) {
    return e.getStatusCode() == 404;
  }
}
