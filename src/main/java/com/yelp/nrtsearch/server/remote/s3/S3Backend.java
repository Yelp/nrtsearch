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

import static com.yelp.nrtsearch.server.utils.TimeStringUtil.generateTimeStringSec;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.utils.ZipUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  ;
  static final String INDEX_RESOURCE_PREFIX_FORMAT = "%s/%s/%s/";
  static final String INDEX_BACKEND_FILE_FORMAT = "%s-%s-%s";
  static final String POINT_STATE_FILE_FORMAT = "%s-%s-%s";
  static final String POINT_STATE = "point_state";
  static final String DATA = "data";
  static final String CURRENT_VERSION = "_current";

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
   * Pair of file names, one for the local file and one for the backend file.
   *
   * @param fileName local file name
   * @param backendFileName backend file name
   */
  record FileNamePair(String fileName, String backendFileName) {}

  /**
   * Constructor.
   *
   * @param configuration configuration
   * @param s3 s3 client
   */
  public S3Backend(LuceneServerConfiguration configuration, AmazonS3 s3) {
    this(configuration.getBucketName(), configuration.getSavePluginBeforeUnzip(), s3);
  }

  /**
   * Constructor.
   *
   * @param serviceBucket bucket name
   * @param savePluginBeforeUnzip save plugin before unzipping
   * @param s3 s3 client
   */
  public S3Backend(String serviceBucket, boolean savePluginBeforeUnzip, AmazonS3 s3) {
    this.s3 = s3;
    this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_S3_THREADS);
    this.saveBeforeUnzip = savePluginBeforeUnzip;
    this.serviceBucket = serviceBucket;
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
    if (resourceType == IndexResourceType.POINT_STATE) {
      String prefix = indexPointStateResourcePrefix(service, indexIdentifier);
      return currentResourceExists(prefix);
    } else {
      String resource = getResourceName(indexIdentifier, resourceType);
      try {
        getVersionString(service, resource, LATEST_VERSION);
        return true;
      } catch (IllegalArgumentException e) {
        return false;
      }
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

  @Override
  public void uploadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException {
    List<FileNamePair> fileList = getFileNamePairs(files);
    String backendPrefix = indexDataResourcePrefix(service, indexIdentifier);
    List<Upload> uploadList = new LinkedList<>();
    for (FileNamePair pair : fileList) {
      String backendKey = backendPrefix + pair.backendFileName;
      Path localFile = indexDir.resolve(pair.fileName);
      PutObjectRequest request =
          new PutObjectRequest(serviceBucket, backendKey, localFile.toFile());
      request.setGeneralProgressListener(
          new S3ProgressListenerImpl(service, indexIdentifier, "upload_index_files"));
      Upload upload = transferManager.upload(request);
      uploadList.add(upload);
    }

    boolean hasFailure = false;
    Throwable failureCause = null;
    for (Upload upload : uploadList) {
      if (hasFailure) {
        upload.abort();
      } else {
        try {
          upload.waitForUploadResult();
        } catch (Throwable t) {
          hasFailure = true;
          failureCause = t;
        }
      }
    }
    if (hasFailure) {
      throw new IOException("Error while downloading index files from s3. ", failureCause);
    }
  }

  @Override
  public void downloadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException {
    List<FileNamePair> fileList = getFileNamePairs(files);
    String backendPrefix = indexDataResourcePrefix(service, indexIdentifier);
    List<Download> downloadList = new LinkedList<>();
    boolean hasFailure = false;
    Throwable failureCause = null;
    for (FileNamePair pair : fileList) {
      String backendKey = backendPrefix + pair.backendFileName;
      Path localFile = indexDir.resolve(pair.fileName);
      GetObjectRequest request = new GetObjectRequest(serviceBucket, backendKey);
      request.setGeneralProgressListener(
          new S3ProgressListenerImpl(service, indexIdentifier, "download_index_files"));
      try {
        Download download = transferManager.download(request, localFile.toFile());
        downloadList.add(download);
      } catch (Throwable t) {
        hasFailure = true;
        failureCause = t;
        break;
      }
    }

    for (Download download : downloadList) {
      if (hasFailure) {
        download.abort();
      } else {
        try {
          download.waitForCompletion();
        } catch (Throwable t) {
          hasFailure = true;
          failureCause = t;
        }
      }
    }
    if (hasFailure) {
      throw new IOException("Error while downloading index files from s3. ", failureCause);
    }
  }

  static String indexDataResourcePrefix(String service, String indexIdentifier) {
    return String.format(INDEX_RESOURCE_PREFIX_FORMAT, service, indexIdentifier, DATA);
  }

  static List<FileNamePair> getFileNamePairs(Map<String, NrtFileMetaData> files) {
    List<FileNamePair> fileList = new LinkedList<>();
    for (Map.Entry<String, NrtFileMetaData> entry : files.entrySet()) {
      String fileName = entry.getKey();
      NrtFileMetaData fileMetaData = entry.getValue();
      fileList.add(new FileNamePair(fileName, getIndexBackendFileName(fileName, fileMetaData)));
    }
    return fileList;
  }

  static String getIndexBackendFileName(String fileName, NrtFileMetaData fileMetaData) {
    return String.format(
        INDEX_BACKEND_FILE_FORMAT, fileMetaData.timeString, fileMetaData.primaryId, fileName);
  }

  @Override
  public void uploadPointState(String service, String indexIdentifier, NrtPointState nrtPointState)
      throws IOException {
    String prefix = indexPointStateResourcePrefix(service, indexIdentifier);
    String fileName = getPointStateFileName(nrtPointState);
    String backendKey = prefix + fileName;
    String jsonString = OBJECT_MAPPER.writeValueAsString(nrtPointState);
    byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    PutObjectRequest request =
        new PutObjectRequest(serviceBucket, backendKey, new ByteArrayInputStream(bytes), metadata);
    request.setGeneralProgressListener(
        new S3ProgressListenerImpl(service, indexIdentifier, "upload_point_state"));
    s3.putObject(request);

    setCurrentResource(prefix, fileName);
  }

  @Override
  public NrtPointState downloadPointState(String service, String indexIdentifier)
      throws IOException {
    String prefix = indexPointStateResourcePrefix(service, indexIdentifier);
    String fileName = getCurrentResourceName(prefix);
    String backendKey = prefix + fileName;
    GetObjectRequest request = new GetObjectRequest(serviceBucket, backendKey);
    request.setGeneralProgressListener(
        new S3ProgressListenerImpl(service, indexIdentifier, "download_point_state"));
    S3Object s3Object = s3.getObject(request);
    String jsonString = IOUtils.toString(s3Object.getObjectContent(), StandardCharsets.UTF_8);
    return OBJECT_MAPPER.readValue(jsonString, NrtPointState.class);
  }

  static String indexPointStateResourcePrefix(String service, String indexIdentifier) {
    return String.format(INDEX_RESOURCE_PREFIX_FORMAT, service, indexIdentifier, POINT_STATE);
  }

  static String getPointStateFileName(NrtPointState nrtPointState) {
    String timestamp = generateTimeStringSec();
    return String.format(
        POINT_STATE_FILE_FORMAT, timestamp, nrtPointState.primaryId, nrtPointState.version);
  }

  String getCurrentResourceName(String prefix) throws IOException {
    String key = prefix + CURRENT_VERSION;
    S3Object s3Object = s3.getObject(serviceBucket, key);
    return IOUtils.toString(s3Object.getObjectContent(), StandardCharsets.UTF_8);
  }

  void setCurrentResource(String prefix, String version) {
    String key = prefix + CURRENT_VERSION;
    byte[] bytes = version.getBytes(StandardCharsets.UTF_8);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    PutObjectRequest request =
        new PutObjectRequest(serviceBucket, key, new ByteArrayInputStream(bytes), metadata);
    s3.putObject(request);
    logger.info("Set current resource - prefix: {}, version: {}", prefix, version);
  }

  boolean currentResourceExists(String prefix) {
    String key = prefix + CURRENT_VERSION;
    return s3.doesObjectExist(serviceBucket, key);
  }

  @VisibleForTesting
  static String getResourceName(String indexIdentifier, IndexResourceType resourceType) {
    return switch (resourceType) {
      case WARMING_QUERIES -> indexIdentifier + WARMING_QUERIES_RESOURCE_SUFFIX;
      default -> throw new IllegalArgumentException("Unknown resource type: " + resourceType);
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
