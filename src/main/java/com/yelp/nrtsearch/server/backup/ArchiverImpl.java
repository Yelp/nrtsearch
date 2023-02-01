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

import static com.yelp.nrtsearch.server.utils.S3Downloader.NUM_S3_THREADS;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.google.inject.Inject;
import com.yelp.nrtsearch.server.utils.S3Downloader;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation is deprecated. IndexArchiver is the current implementation that facilitates
 * incremental backups and faster downloads.
 */
@Deprecated
public class ArchiverImpl implements Archiver {
  public static final String DELIMITER = "/";
  private static final Logger logger = LoggerFactory.getLogger(ArchiverImpl.class);
  private static final String CURRENT_VERSION_NAME = "current";
  private static final String TMP_SUFFIX = ".tmp";

  private final AmazonS3 s3;
  private final String bucketName;
  private final Path archiverDirectory;
  private final Tar tar;
  private final VersionManager versionManger;
  private final TransferManager transferManager;
  private final ThreadPoolExecutor executor =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);
  private final S3Downloader s3Downloader;
  private final boolean downloadAsStream;

  @Inject
  public ArchiverImpl(
      final AmazonS3 s3,
      final String bucketName,
      final Path archiverDirectory,
      final Tar tar,
      final boolean downloadAsStream) {
    this.s3 = s3;
    this.transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(false)
            .build();
    this.bucketName = bucketName;
    this.archiverDirectory = archiverDirectory;
    this.tar = tar;
    this.versionManger = new VersionManager(s3, bucketName);
    this.s3Downloader = new S3Downloader(s3, executor);
    this.downloadAsStream = downloadAsStream;
  }

  public ArchiverImpl(
      final AmazonS3 s3, final String bucketName, final Path archiverDirectory, final Tar tar) {
    this(s3, bucketName, archiverDirectory, tar, false);
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
    final Path relativeVersionDirectory = Paths.get(versionHash);
    logger.info(
        "Downloading resource {} for service {} version {} to directory {}",
        resource,
        serviceName,
        versionHash,
        versionDirectory);
    getVersionContent(serviceName, resource, versionHash, versionDirectory);
    try {
      logger.info("Point current version symlink to new resource {}", resource);
      Files.createSymbolicLink(tempCurrentLink, relativeVersionDirectory);
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
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    List<VersionedResource> resources = new ArrayList<>();
    ListObjectsRequest listObjectsRequest =
        new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(serviceName + DELIMITER + resource + DELIMITER)
            .withDelimiter(DELIMITER);

    List<S3ObjectSummary> objects = s3.listObjects(listObjectsRequest).getObjectSummaries();

    for (S3ObjectSummary object : objects) {
      String key = object.getKey();
      String[] prefix = key.split(DELIMITER);
      String versionHash = prefix[prefix.length - 1];
      VersionedResource versionedResource =
          VersionedResource.builder()
              .setServiceName(serviceName)
              .setResourceName(resource)
              .setVersionHash(versionHash)
              .setCreationTimestamp(object.getLastModified().toInstant())
              .createVersionedResource();
      resources.add(versionedResource);
    }
    return resources;
  }

  @Override
  public String upload(
      final String serviceName,
      final String resource,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException {
    if (!Files.exists(sourceDir)) {
      throw new IOException(
          String.format(
              "Source directory %s, for service %s, and resource %s does not exist",
              sourceDir, serviceName, resource));
    }
    if (stream) {
      return uploadAsStream(
          serviceName, resource, sourceDir, filesToInclude, parentDirectoriesToInclude);
    } else {
      return uploadAsFile(
          serviceName, resource, sourceDir, filesToInclude, parentDirectoriesToInclude);
    }
  }

  private String uploadAsFile(
      final String serviceName,
      final String resource,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException {
    if (!Files.exists(archiverDirectory)) {
      logger.info("Archiver directory doesn't exist: " + archiverDirectory + " creating new ");
      Files.createDirectories(archiverDirectory);
    }
    Path destPath = archiverDirectory.resolve(getTmpName());
    try {
      tar.buildTar(sourceDir, destPath, filesToInclude, parentDirectoriesToInclude);
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
    request.setGeneralProgressListener(
        new ContentDownloaderImpl.S3ProgressListenerImpl(serviceName, resource, "upload"));
    Upload upload = transferManager.upload(request);
    try {
      upload.waitForUploadResult();
      logger.info("Upload completed ");
    } catch (InterruptedException e) {
      throw new IOException("Error while uploading to s3. ", e);
    }
  }

  private String uploadAsStream(
      final String serviceName,
      final String resource,
      Path sourceDir,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude)
      throws IOException {
    String versionHash = UUID.randomUUID().toString();
    final String absoluteResourcePath =
        String.format("%s/%s/%s", serviceName, resource, versionHash);
    long uncompressedSize =
        getTotalSize(sourceDir.toString(), filesToInclude, parentDirectoriesToInclude);
    logger.info("Uploading: " + absoluteResourcePath);
    logger.info("Uncompressed total size: " + uncompressedSize);
    TarUploadOutputStream uploadStream = null;
    try {
      uploadStream =
          new TarUploadOutputStream(
              bucketName,
              absoluteResourcePath,
              uncompressedSize,
              transferManager.getAmazonS3Client(),
              executor);
      tar.buildTar(sourceDir, uploadStream, filesToInclude, parentDirectoriesToInclude);
      uploadStream.complete();
    } catch (Exception e) {
      if (uploadStream != null) {
        uploadStream.cancel();
      }
      throw new IOException("Error uploading tar to s3", e);
    }
    return versionHash;
  }

  private long getTotalSize(
      String filePath,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude) {
    File file = new File(filePath);
    long totalSize = 0;
    if (file.isFile()
        && TarImpl.shouldIncludeFile(file, filesToInclude, parentDirectoriesToInclude)) {
      totalSize += file.length();
    } else if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        totalSize += getTotalSize(f.getAbsolutePath(), filesToInclude, parentDirectoriesToInclude);
      }
    }
    return totalSize;
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
    try (final S3Object s3Object = s3.getObject(bucketName, absoluteResourcePath)) {
      return IOUtils.toString(s3Object.getObjectContent());
    }
  }

  private void getVersionContent(
      final String serviceName, final String resource, final String hash, final Path destDirectory)
      throws IOException {
    final String absoluteResourcePath = String.format("%s/%s/%s", serviceName, resource, hash);
    final Path parentDirectory = destDirectory.getParent();
    final Path tmpFile = parentDirectory.resolve(getTmpName());

    final InputStream s3InputStream;
    if (downloadAsStream) {
      // Stream the file download from s3 instead of writing to a file first
      s3InputStream = s3Downloader.downloadFromS3Path(bucketName, absoluteResourcePath);
    } else {
      Download download =
          transferManager.download(
              new GetObjectRequest(bucketName, absoluteResourcePath),
              tmpFile.toFile(),
              new ContentDownloaderImpl.S3ProgressListenerImpl(serviceName, resource, "download"));
      try {
        download.waitForCompletion();
        logger.info("S3 Download complete");
      } catch (InterruptedException e) {
        throw new IOException("S3 Download failed", e);
      }

      s3InputStream = new FileInputStream(tmpFile.toFile());
    }

    final InputStream compressorInputStream;
    if (tar.getCompressionMode().equals(Tar.CompressionMode.LZ4)) {
      compressorInputStream = new LZ4FrameInputStream(s3InputStream);
    } else {
      compressorInputStream = new GzipCompressorInputStream(s3InputStream, true);
    }
    try (final TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(compressorInputStream); ) {
      if (Files.exists(destDirectory)) {
        logger.info("Directory {} already exists, not re-downloading from Archiver", destDirectory);
        return;
      }
      final Path tmpDirectory = parentDirectory.resolve(getTmpName());
      try {
        long tarBefore = System.nanoTime();
        logger.info("Extract tar started...");
        tar.extractTar(tarArchiveInputStream, tmpDirectory);
        long tarAfter = System.nanoTime();
        logger.info(
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
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return versionManger.deleteVersion(serviceName, resource, versionHash);
  }

  @Override
  public boolean deleteLocalFiles(String resource) {
    return IndexArchiver.deleteLocalResourceFiles(resource, archiverDirectory);
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
}
