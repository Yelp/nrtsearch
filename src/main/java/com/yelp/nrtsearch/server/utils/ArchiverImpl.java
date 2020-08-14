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
package com.yelp.nrtsearch.server.utils;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.google.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiverImpl implements Archiver {
  private static final int NUM_S3_THREADS = 20;
  public static final String DELIMITER = "/";
  private static Logger logger = LoggerFactory.getLogger(ArchiverImpl.class);
  private static final String CURRENT_VERSION_NAME = "current";
  private static final String TMP_SUFFIX = ".tmp";

  private final AmazonS3 s3;
  private final String bucketName;
  private final Path archiverDirectory;
  private final Tar tar;
  private final VersionManager versionManger;
  private final TransferManager transferManager;

  @Inject
  public ArchiverImpl(
      final AmazonS3 s3, final String bucketName, final Path archiverDirectory, final Tar tar) {
    this.s3 = s3;
    this.transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3)
            .withExecutorFactory(() -> Executors.newFixedThreadPool(NUM_S3_THREADS))
            .build();
    this.bucketName = bucketName;
    this.archiverDirectory = archiverDirectory;
    this.tar = tar;
    this.versionManger = new VersionManager(s3, bucketName);
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    if (!Files.exists(archiverDirectory)) {
      logger.info("Archiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }

    final String latestVersion = getVersionString(serviceName, resource, "_latest_version");
    final String versionHash = getVersionString(serviceName, resource, latestVersion);
    final Path resourceDestDirectory = archiverDirectory.resolve(resource);
    final Path versionDirectory = resourceDestDirectory.resolve(versionHash);
    final Path currentDirectory = resourceDestDirectory.resolve("current");
    final Path tempCurrentLink = resourceDestDirectory.resolve(getTmpName());
    final Path relativVersionDirectory = Paths.get(versionHash);
    logger.info(
        "Downloading resource {} for service {} version {} to directory {}",
        resource,
        serviceName,
        versionHash,
        versionDirectory);
    getVersionContent(serviceName, resource, versionHash, versionDirectory);
    try {
      logger.info("Point current version symlink to new resource {}", resource);
      Files.createSymbolicLink(tempCurrentLink, relativVersionDirectory);
      Files.move(tempCurrentLink, currentDirectory, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      if (Files.exists(tempCurrentLink)) {
        FileUtils.deleteDirectory(tempCurrentLink.toFile());
      }
    }
    cleanupFiles(versionHash, resourceDestDirectory);
    return currentDirectory;
  }

  @Override
  public List<String> getResources(String serviceName) {
    List<String> resources = new ArrayList<>();
    ListObjectsRequest listObjectsRequest =
        new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(serviceName + DELIMITER)
            .withDelimiter(DELIMITER);
    List<String> resourcePrefixes = s3.listObjects(listObjectsRequest).getCommonPrefixes();
    for (String resource : resourcePrefixes) {
      String[] prefix = resource.split(DELIMITER);
      String potentialResourceName = prefix[prefix.length - 1];
      if (!potentialResourceName.equals("_version")) {
        resources.add(potentialResourceName);
      }
    }
    return resources;
  }

  @Override
  public String upload(final String serviceName, final String resource, Path sourceDir)
      throws IOException {
    if (!Files.exists(sourceDir)) {
      throw new IOException(
          String.format(
              "Source directory %s, for service %s, and resource %s does not exist",
              sourceDir, serviceName, resource));
    }

    if (!Files.exists(archiverDirectory)) {
      logger.info("Archiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }
    Path destPath = archiverDirectory.resolve(getTmpName());
    try {
      tar.buildTar(sourceDir, destPath);
      String versionHash = UUID.randomUUID().toString();
      uploadTarWithMetadata(serviceName, resource, versionHash, destPath);
      return versionHash;
    } finally {
      Files.deleteIfExists(destPath);
    }
  }

  private void uploadTarWithMetadata(
      String serviceName, String resource, String versionHash, Path path) throws IOException {
    final String absoluteResourcePath =
        String.format("%s/%s/%s", serviceName, resource, versionHash);
    PutObjectRequest request =
        new PutObjectRequest(bucketName, absoluteResourcePath, path.toFile());
    request.setGeneralProgressListener(new S3ProgressListenerImpl(serviceName, resource, "upload"));
    Upload upload = transferManager.upload(request);
    try {
      upload.waitForUploadResult();
      logger.info("Upload completed ");
    } catch (InterruptedException e) {
      throw new IOException("Error while uploading to s3. ", e);
    }
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String resourceHash)
      throws IOException {
    return versionManger.blessVersion(serviceName, resource, resourceHash);
  }

  private String getVersionString(
      final String serviceName, final String resource, final String version) throws IOException {
    final String absoluteResourcePath =
        String.format("%s/_version/%s/%s", serviceName, resource, version);
    try (final S3Object s3Object = s3.getObject(bucketName, absoluteResourcePath); ) {
      final String versionPath = IOUtils.toString(s3Object.getObjectContent());
      return versionPath;
    }
  }

  private void getVersionContent(
      final String serviceName, final String resource, final String hash, final Path destDirectory)
      throws IOException {
    final String absoluteResourcePath = String.format("%s/%s/%s", serviceName, resource, hash);
    final Path parentDirectory = destDirectory.getParent();
    final Path tmpFile = parentDirectory.resolve(getTmpName());

    Download download =
        transferManager.download(
            new GetObjectRequest(bucketName, absoluteResourcePath),
            tmpFile.toFile(),
            new S3ProgressListenerImpl(serviceName, resource, "download"));
    try {
      download.waitForCompletion();
      logger.info("S3 Download complete");
    } catch (InterruptedException e) {
      throw new IOException("S3 Download failed", e);
    }

    final InputStream s3InputStreem = new FileInputStream(tmpFile.toFile());
    final InputStream compressorInputStream;
    if (tar.getCompressionMode().equals(Tar.CompressionMode.LZ4)) {
      compressorInputStream = new LZ4FrameInputStream(s3InputStreem);
    } else {
      compressorInputStream = new GzipCompressorInputStream(s3InputStreem, true);
    }
    try (final TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(compressorInputStream); ) {
      if (Files.exists(destDirectory)) {
        logger.info("Directory {} already exists, not re-downloading from Archiver", destDirectory);
        return;
      }
      final Path tmpDirectory = parentDirectory.resolve(getTmpName());
      try {
        tar.extractTar(tarArchiveInputStream, tmpDirectory);
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
  }

  private String getTmpName() {
    return UUID.randomUUID().toString() + TMP_SUFFIX;
  }

  private void cleanupFiles(final String versionHash, final Path resourceDestDirectory)
      throws IOException {
    final DirectoryStream.Filter<Path> filter =
        entry -> {
          final String fileName = entry.getFileName().toString();
          // Ignore the current version
          if (CURRENT_VERSION_NAME.equals(fileName)) {
            return false;
          }
          // Ignore the current version hash
          if (versionHash.equals(fileName)) {
            return false;
          }
          // Ignore non-directories
          if (!Files.isDirectory(entry)) {
            logger.warn("Unexpected non-directory entry found while cleaning up: {}", fileName);
            return false;
          }
          // Ignore version names that aren't hex encoded
          try {
            Hex.decodeHex(fileName.toCharArray());
          } catch (DecoderException e) {
            logger.warn(
                "Not cleaning up directory because name doesn't match pattern: {}", fileName);
            return false;
          }
          return true;
        };
    try (final DirectoryStream<Path> stream =
        Files.newDirectoryStream(resourceDestDirectory, filter)) {
      for (final Path entry : stream) {
        logger.info("Cleaning up old directory: {}", entry);
        FileUtils.deleteDirectory(entry.toFile());
      }
    }
  }

  private static class S3ProgressListenerImpl implements S3ProgressListener {
    private static Logger logger = LoggerFactory.getLogger(S3ProgressListenerImpl.class);

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
