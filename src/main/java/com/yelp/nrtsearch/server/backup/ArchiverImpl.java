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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.google.inject.Inject;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
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
    request.setGeneralProgressListener(new S3ProgressListenerImpl(serviceName, resource, "upload"));
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
      logger.info("Full object size: " + fullMetadata.getContentLength());

      // get metadata for the 1st file part, needed to find the total number of parts
      ObjectMetadata partMetadata =
          transferManager.getAmazonS3Client().getObjectMetadata(metadataRequest.withPartNumber(1));
      int numParts = partMetadata.getPartCount() != null ? partMetadata.getPartCount() : 1;
      logger.info("Object parts: " + numParts);
      s3InputStream = getObjectStream(absoluteResourcePath, numParts);
      logger.info("Object streaming started...");
    } else {
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
    private static final Logger logger = LoggerFactory.getLogger(S3ProgressListenerImpl.class);

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

  protected AmazonS3 providesAmazonS3(LuceneServerConfiguration luceneServerConfiguration) {
    if (luceneServerConfiguration
        .getBotoCfgPath()
        .equals(LuceneServerConfiguration.DEFAULT_BOTO_CFG_PATH.toString())) {
      return AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration("dummyService", "dummyRegion"))
          .build();
    } else {
      Path botoCfgPath = Paths.get(luceneServerConfiguration.getBotoCfgPath());
      final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
      final AWSCredentialsProvider awsCredentialsProvider =
          new ProfileCredentialsProvider(profilesConfigFile, "default");
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
      String region = s3ClientInterim.getBucketLocation(luceneServerConfiguration.getBucketName());
      // In useast-1, the region is returned as "US" which is an equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html#US_Standard
      // However, this causes an UnknownHostException so we override it to the full region name
      if (region.equals("US")) {
        region = "us-east-1";
      }
      String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
      System.out.println("***********");
      System.out.println("***********");
      System.out.println(String.format("S3 ServiceEndpoint: %s", serviceEndpoint));
      System.out.println("***********");
      System.out.println("***********");
      return AmazonS3ClientBuilder.standard()
          .withCredentials(awsCredentialsProvider)
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
          .build();
    }
  }
}
