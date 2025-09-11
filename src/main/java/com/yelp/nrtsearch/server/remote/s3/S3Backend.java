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

import static com.yelp.nrtsearch.server.utils.TimeStringUtils.generateTimeStringSec;

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
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.StateUtils;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.server.utils.ZipUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Path;
import java.time.Instant;
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
  static final String GLOBAL_STATE_PREFIX_FORMAT = "%s/%s/";
  static final String INDEX_RESOURCE_PREFIX_FORMAT = "%s/%s/%s/";
  static final String GLOBAL_STATE_FILE_FORMAT = "%s-%s";
  static final String INDEX_STATE_FILE_FORMAT = "%s-%s";
  static final String INDEX_BACKEND_FILE_FORMAT = "%s-%s-%s";
  static final String POINT_STATE_FILE_FORMAT = "%s-%s-%s";
  static final String WARMING_QUERIES_FILE_FORMAT = "%s-%s";
  static final String GLOBAL_STATE = "global_state";
  static final String INDEX_STATE = "state";
  static final String POINT_STATE = "point_state";
  static final String DATA = "data";
  static final String WARMING = "warming";
  public static final String CURRENT_VERSION = "_current";

  private static final Logger logger = LoggerFactory.getLogger(S3Backend.class);
  private static final String ZIP_EXTENSION = ".zip";

  public static final int NUM_S3_THREADS = 20;

  private final ThreadPoolExecutor executor;
  private final boolean saveBeforeUnzip;
  private final AmazonS3 s3;
  private final String serviceBucket;
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
  public S3Backend(NrtsearchConfig configuration, AmazonS3 s3) {
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
    this.transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(false)
            .build();
  }

  public AmazonS3 getS3() {
    return s3;
  }

  @Override
  public String downloadPluginIfNeeded(String pluginNameOrPath, Path destPath) {
    if (S3Util.isValidS3FilePath(pluginNameOrPath) && pluginNameOrPath.endsWith(ZIP_EXTENSION)) {
      logger.info("Downloading plugin: {}", pluginNameOrPath);
      ZipUtils.extractZip(downloadFromS3Path(pluginNameOrPath), destPath, saveBeforeUnzip);
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
    return downloadFromS3Path(s3URI.getBucket(), s3URI.getKey(), true);
  }

  /**
   * Download a file from the specified bucket and key.
   *
   * @param bucketName Bucket name in S3
   * @param absoluteResourcePath Key for the file in the bucket
   * @param verbose log verbose output
   * @return {@link InputStream} of the file being streamed
   * @throws IllegalArgumentException if bucket or path not found
   */
  public InputStream downloadFromS3Path(
      String bucketName, String absoluteResourcePath, boolean verbose) {
    if (verbose) {
      logger.info("Downloading {} from bucket {}", absoluteResourcePath, bucketName);
    }
    final InputStream s3InputStream;
    // Stream the file download from s3 instead of writing to a file first
    GetObjectMetadataRequest metadataRequest =
        new GetObjectMetadataRequest(bucketName, absoluteResourcePath);
    long fullObjectSize;
    try {
      ObjectMetadata fullMetadata = s3.getObjectMetadata(metadataRequest);
      fullObjectSize = fullMetadata.getContentLength();
    } catch (AmazonS3Exception e) {
      if (isNotFoundException(e)) {
        String error =
            String.format("Object s3://%s/%s not found", bucketName, absoluteResourcePath);
        throw new IllegalArgumentException(error, e);
      }
      throw e;
    }

    if (verbose) {
      logger.info("Full object size: " + fullObjectSize);
    } else {
      logger.info("Downloading {}, size: {}", absoluteResourcePath, fullObjectSize);
    }

    // get metadata for the 1st file part, needed to find the total number of parts
    ObjectMetadata partMetadata = s3.getObjectMetadata(metadataRequest.withPartNumber(1));
    int numParts = partMetadata.getPartCount() != null ? partMetadata.getPartCount() : 1;
    if (verbose) {
      logger.info("Object parts: " + numParts);
    }
    s3InputStream = getObjectStream(bucketName, absoluteResourcePath, numParts);
    if (verbose) {
      logger.info("Object streaming started...");
    }
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
  public boolean exists(String service, GlobalResourceType resourceType) throws IOException {
    String prefix =
        switch (resourceType) {
          case GLOBAL_STATE -> getGlobalStateResourcePrefix(service);
        };
    return currentResourceExists(prefix);
  }

  @Override
  public boolean exists(String service, String indexIdentifier, IndexResourceType resourceType)
      throws IOException {
    String prefix =
        switch (resourceType) {
          case WARMING_QUERIES -> getIndexResourcePrefix(service, indexIdentifier, WARMING);
          case POINT_STATE -> getIndexResourcePrefix(service, indexIdentifier, POINT_STATE);
          case INDEX_STATE -> getIndexResourcePrefix(service, indexIdentifier, INDEX_STATE);
        };
    return currentResourceExists(prefix);
  }

  /**
   * Get the S3 prefix for the service global resource.
   *
   * @param service service name
   * @return S3 prefix
   */
  public static String getGlobalStateResourcePrefix(String service) {
    return String.format(GLOBAL_STATE_PREFIX_FORMAT, service, GLOBAL_STATE);
  }

  /**
   * Get the S3 prefix for the specified index resource type.
   *
   * @param service service name
   * @param indexIdentifier index identifier
   * @param resourceType resource type
   * @return S3 prefix
   */
  public static String getIndexResourcePrefix(
      String service, String indexIdentifier, IndexResourceType resourceType) {
    return switch (resourceType) {
      case WARMING_QUERIES -> getIndexResourcePrefix(service, indexIdentifier, WARMING);
      case POINT_STATE -> getIndexResourcePrefix(service, indexIdentifier, POINT_STATE);
      case INDEX_STATE -> getIndexResourcePrefix(service, indexIdentifier, INDEX_STATE);
    };
  }

  /**
   * Get the S3 prefix for index data files.
   *
   * @param service service name
   * @param indexIdentifier index identifier
   * @return S3 prefix
   */
  public static String getIndexDataPrefix(String service, String indexIdentifier) {
    return getIndexResourcePrefix(service, indexIdentifier, DATA);
  }

  private static String getIndexResourcePrefix(
      String service, String indexIdentifier, String resourceType) {
    return String.format(INDEX_RESOURCE_PREFIX_FORMAT, service, indexIdentifier, resourceType);
  }

  @Override
  public void uploadGlobalState(String service, byte[] data) throws IOException {
    String prefix = getGlobalStateResourcePrefix(service);
    String fileName = getGlobalStateFileName();
    uploadResource(prefix, fileName, service, GLOBAL_STATE, data);
  }

  @Override
  public InputStream downloadGlobalState(String service) throws IOException {
    String prefix = getGlobalStateResourcePrefix(service);
    return downloadResource(prefix);
  }

  @Override
  public void uploadIndexState(String service, String indexIdentifier, byte[] data)
      throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.INDEX_STATE);
    String fileName = getIndexStateFileName();
    uploadResource(prefix, fileName, service, indexIdentifier, data);
  }

  @Override
  public InputStream downloadIndexState(String service, String indexIdentifier) throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.INDEX_STATE);
    return downloadResource(prefix);
  }

  @Override
  public void uploadWarmingQueries(String service, String indexIdentifier, byte[] data)
      throws IOException {
    String prefix =
        getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.WARMING_QUERIES);
    String fileName = getWarmingQueriesFileName();
    uploadResource(prefix, fileName, service, indexIdentifier, data);
  }

  @Override
  public InputStream downloadWarmingQueries(String service, String indexIdentifier)
      throws IOException {
    String prefix =
        getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.WARMING_QUERIES);
    return downloadResource(prefix);
  }

  @Override
  public void uploadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException {
    List<FileNamePair> fileList = getFileNamePairs(files);
    String backendPrefix = getIndexDataPrefix(service, indexIdentifier);
    List<Upload> uploadList = new LinkedList<>();
    boolean hasFailure = false;
    Throwable failureCause = null;
    for (FileNamePair pair : fileList) {
      String backendKey = backendPrefix + pair.backendFileName;
      Path localFile = indexDir.resolve(pair.fileName);
      PutObjectRequest request =
          new PutObjectRequest(serviceBucket, backendKey, localFile.toFile());
      request.setGeneralProgressListener(
          new S3ProgressListenerImpl(service, indexIdentifier, "upload_index_files"));
      try {
        Upload upload = transferManager.upload(request);
        uploadList.add(upload);
      } catch (Throwable t) {
        hasFailure = true;
        failureCause = t;
        break;
      }
    }

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
    String backendPrefix = getIndexDataPrefix(service, indexIdentifier);
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

  @Override
  public InputStream downloadIndexFile(
      String service, String indexIdentifier, String fileName, NrtFileMetaData fileMetaData)
      throws IOException {
    String backendFileName = getIndexBackendFileName(fileName, fileMetaData);
    String backendPrefix = getIndexDataPrefix(service, indexIdentifier);
    String backendKey = backendPrefix + backendFileName;
    return downloadFromS3Path(serviceBucket, backendKey, false);
  }

  @VisibleForTesting
  static List<FileNamePair> getFileNamePairs(Map<String, NrtFileMetaData> files) {
    List<FileNamePair> fileList = new LinkedList<>();
    for (Map.Entry<String, NrtFileMetaData> entry : files.entrySet()) {
      String fileName = entry.getKey();
      NrtFileMetaData fileMetaData = entry.getValue();
      fileList.add(new FileNamePair(fileName, getIndexBackendFileName(fileName, fileMetaData)));
    }
    return fileList;
  }

  @Override
  public void uploadPointState(
      String service, String indexIdentifier, NrtPointState nrtPointState, byte[] data)
      throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.POINT_STATE);
    String fileName = getPointStateFileName(nrtPointState);
    String backendKey = prefix + fileName;
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(data.length);
    PutObjectRequest request =
        new PutObjectRequest(serviceBucket, backendKey, new ByteArrayInputStream(data), metadata);
    request.setGeneralProgressListener(
        new S3ProgressListenerImpl(service, indexIdentifier, "upload_point_state"));
    s3.putObject(request);

    setCurrentResource(prefix, fileName);
  }

  @Override
  public InputStreamWithTimestamp downloadPointState(String service, String indexIdentifier)
      throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.POINT_STATE);
    String fileName = getCurrentResourceName(prefix);
    String backendKey = prefix + fileName;
    String timeString = getTimeStringFromPointStateFileName(fileName);
    Instant timestamp = TimeStringUtils.parseTimeStringSec(timeString);
    return new InputStreamWithTimestamp(
        downloadFromS3Path(serviceBucket, backendKey, true), timestamp);
  }

  private void uploadResource(
      String prefix, String fileName, String service, String progressIdentifier, byte[] data)
      throws IOException {
    String backendKey = prefix + fileName;
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(data.length);
    PutObjectRequest request =
        new PutObjectRequest(serviceBucket, backendKey, new ByteArrayInputStream(data), metadata);
    request.setGeneralProgressListener(
        new S3ProgressListenerImpl(service, progressIdentifier, "upload_data"));
    Upload upload = transferManager.upload(request);
    try {
      upload.waitForUploadResult();
    } catch (InterruptedException e) {
      throw new IOException("Error uploading to S3", e);
    }

    setCurrentResource(prefix, fileName);
  }

  private InputStream downloadResource(String prefix) throws IOException {
    String fileName = getCurrentResourceName(prefix);
    String backendKey = prefix + fileName;
    return downloadFromS3Path(serviceBucket, backendKey, true);
  }

  @VisibleForTesting
  static String getGlobalStateFileName() {
    String timestamp = generateTimeStringSec();
    return String.format(GLOBAL_STATE_FILE_FORMAT, timestamp, UUID.randomUUID());
  }

  /**
   * Get file name to use to store index state in S3. The file name will be unique for each call.
   *
   * @return file name
   */
  public static String getIndexStateFileName() {
    String timestamp = generateTimeStringSec();
    return String.format(INDEX_STATE_FILE_FORMAT, timestamp, UUID.randomUUID());
  }

  /**
   * Get file name to use to store point state in S3. The file name will depend on the point state
   * index version.
   *
   * @param nrtPointState
   * @return
   */
  public static String getPointStateFileName(NrtPointState nrtPointState) {
    String timestamp = generateTimeStringSec();
    return String.format(
        POINT_STATE_FILE_FORMAT, timestamp, nrtPointState.primaryId, nrtPointState.version);
  }

  /**
   * Extract the time string from a point state file name.
   *
   * @param pointStateFileName point state file name
   * @return time string
   */
  public static String getTimeStringFromPointStateFileName(String pointStateFileName) {
    String[] parts = pointStateFileName.split("-");
    if (parts.length < 3) {
      throw new IllegalArgumentException("Invalid point state file name: " + pointStateFileName);
    }
    return parts[0];
  }

  /**
   * Get the file name to use to store lucene index file in S3.
   *
   * @param fileName local file name
   * @param fileMetaData file metadata
   * @return backend file name
   */
  public static String getIndexBackendFileName(String fileName, NrtFileMetaData fileMetaData) {
    return String.format(
        INDEX_BACKEND_FILE_FORMAT, fileMetaData.timeString, fileMetaData.primaryId, fileName);
  }

  /**
   * Get the file name to use to store warming queries in S3. The file name will be unique for each
   * call.
   *
   * @return file name
   */
  public static String getWarmingQueriesFileName() {
    return String.format(WARMING_QUERIES_FILE_FORMAT, generateTimeStringSec(), UUID.randomUUID());
  }

  /**
   * Get the current blessed resource version for the specified prefix.
   *
   * @param prefix resource prefix
   * @return current resource version
   * @throws IOException if an error occurs while fetching the current resource version
   */
  public String getCurrentResourceName(String prefix) throws IOException {
    String key = prefix + CURRENT_VERSION;
    try {
      S3Object s3Object = s3.getObject(serviceBucket, key);
      byte[] versionBytes = IOUtils.toByteArray(s3Object.getObjectContent());
      return StateUtils.fromUTF8(versionBytes);
    } catch (AmazonS3Exception e) {
      if (isNotFoundException(e)) {
        String error = String.format("Object s3://%s/%s not found", serviceBucket, key);
        throw new IllegalArgumentException(error, e);
      }
      throw e;
    }
  }

  /**
   * Set the current blessed resource version for the specified prefix.
   *
   * @param prefix resource prefix
   * @param version resource version
   */
  public void setCurrentResource(String prefix, String version) {
    String key = prefix + CURRENT_VERSION;
    byte[] bytes = StateUtils.toUTF8(version);
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

  private boolean isNotFoundException(AmazonS3Exception e) {
    return e.getStatusCode() == 404;
  }
}
