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

import static com.yelp.nrtsearch.server.utils.TimeStringUtils.formatTimeStringSec;
import static com.yelp.nrtsearch.server.utils.TimeStringUtils.generateTimeStringSec;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import com.yelp.nrtsearch.server.monitoring.S3DownloadStreamWrapper;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.state.StateUtils;
import com.yelp.nrtsearch.server.utils.GlobalThrottledInputStream;
import com.yelp.nrtsearch.server.utils.GlobalWindowRateLimiter;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.server.utils.ZipUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

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
  public static final S3BackendConfig DEFAULT_CONFIG = new S3BackendConfig(false, 0, 1);

  private static final Logger logger = LoggerFactory.getLogger(S3Backend.class);
  private static final String ZIP_EXTENSION = ".zip";
  private static final int SECONDS_PER_DAY = 60 * 60 * 24;

  private final ExecutorService executor;
  private final int maxExecutorParallelism;
  private final boolean shutdownExecutor;
  private final boolean saveBeforeUnzip;
  private final S3Client s3;
  private final S3AsyncClient s3Async;
  private final String serviceBucket;
  private final S3TransferManager transferManager;
  private final boolean s3Metrics;
  private final GlobalWindowRateLimiter rateLimiter;

  /**
   * Pair of file names, one for the local file and one for the backend file.
   *
   * @param fileName local file name
   * @param backendFileName backend file name
   */
  record FileNamePair(String fileName, String backendFileName) {}

  /**
   * Configuration for S3 backend.
   *
   * <p>Includes metrics and rate limiting settings.
   */
  public static class S3BackendConfig {
    private static final String CONFIG_PREFIX = "remoteConfig.s3.";

    private final boolean metrics;
    private final long rateLimitBytes;
    private final int rateLimitWindowSeconds;

    /**
     * Create S3BackendConfig from NrtsearchConfig.
     *
     * @param configuration configuration
     * @return S3BackendConfig
     */
    public static S3BackendConfig fromConfig(NrtsearchConfig configuration) {
      YamlConfigReader configReader = configuration.getConfigReader();
      boolean metrics = configReader.getBoolean(CONFIG_PREFIX + "metrics", false);
      String rateLimitString = configReader.getString(CONFIG_PREFIX + "rateLimitPerSecond", "0");
      long rateLimitBytes = rateLimitStringToBytes(rateLimitString);
      int rateLimitWindowSeconds =
          configReader.getInteger(CONFIG_PREFIX + "rateLimitWindowSeconds", 1);

      return new S3BackendConfig(metrics, rateLimitBytes, rateLimitWindowSeconds);
    }

    /**
     * Constructor.
     *
     * @param metrics enable s3 metrics
     * @param rateLimitBytes rate limit in bytes
     * @param rateLimitWindowSeconds rate limit window in seconds
     * @throws IllegalArgumentException if rateLimitBytes < 0 or rateLimitWindowSeconds <= 0
     */
    public S3BackendConfig(boolean metrics, long rateLimitBytes, int rateLimitWindowSeconds) {
      if (rateLimitBytes < 0) {
        throw new IllegalArgumentException("rateLimitBytes must be >= 0");
      }
      if (rateLimitWindowSeconds <= 0) {
        throw new IllegalArgumentException("rateLimitWindowSeconds must be > 0");
      }
      this.metrics = metrics;
      this.rateLimitBytes = rateLimitBytes;
      this.rateLimitWindowSeconds = rateLimitWindowSeconds;
    }

    /**
     * Get whether s3 metrics are enabled.
     *
     * @return true if s3 metrics are enabled
     */
    public boolean getMetrics() {
      return metrics;
    }

    /**
     * Get the rate limit in bytes.
     *
     * @return rate limit in bytes
     */
    public long getRateLimitBytes() {
      return rateLimitBytes;
    }

    /**
     * Get the rate limit window in seconds.
     *
     * @return rate limit window in seconds
     */
    public int getRateLimitWindowSeconds() {
      return rateLimitWindowSeconds;
    }

    @VisibleForTesting
    static long rateLimitStringToBytes(String sizeStr) {
      String baseStr = sizeStr;
      long multiplier = 1;
      if (baseStr.length() > 2) {
        String suffix = baseStr.substring(baseStr.length() - 2).toLowerCase();
        switch (suffix) {
          case "kb" -> {
            baseStr = baseStr.substring(0, baseStr.length() - 2);
            multiplier = 1024;
          }
          case "mb" -> {
            baseStr = baseStr.substring(0, baseStr.length() - 2);
            multiplier = 1024 * 1024;
          }
          case "gb" -> {
            baseStr = baseStr.substring(0, baseStr.length() - 2);
            multiplier = 1024 * 1024 * 1024;
          }
        }
      }
      if (baseStr.isEmpty()) {
        throw new IllegalArgumentException("Cannot convert rate limit string: " + sizeStr);
      }
      double baseValue = Double.parseDouble(baseStr);
      return (long) (baseValue * multiplier);
    }
  }

  /**
   * Constructor.
   *
   * @param configuration configuration
   * @param s3 s3 client
   * @param executorFactory executor factory
   */
  public S3Backend(NrtsearchConfig configuration, S3Client s3, ExecutorFactory executorFactory) {
    this(
        configuration.getBucketName(),
        configuration.getSavePluginBeforeUnzip(),
        S3BackendConfig.fromConfig(configuration),
        s3,
        executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE),
        configuration
            .getThreadPoolConfiguration()
            .getThreadPoolSettings(ExecutorFactory.ExecutorType.REMOTE)
            .maxThreads(),
        false);
  }

  /**
   * Constructor for testing with pre-configured S3AsyncClient and TransferManager.
   *
   * @param configuration configuration
   * @param s3 s3 client
   * @param s3Async s3 async client
   * @param transferManager s3 transfer manager
   * @param executorFactory executor factory
   */
  public S3Backend(
      NrtsearchConfig configuration,
      S3Client s3,
      S3AsyncClient s3Async,
      S3TransferManager transferManager,
      ExecutorFactory executorFactory) {
    this(
        configuration.getBucketName(),
        configuration.getSavePluginBeforeUnzip(),
        S3BackendConfig.fromConfig(configuration),
        s3,
        s3Async,
        transferManager,
        executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE),
        configuration
            .getThreadPoolConfiguration()
            .getThreadPoolSettings(ExecutorFactory.ExecutorType.REMOTE)
            .maxThreads(),
        false);
  }

  /**
   * Constructor.
   *
   * @param serviceBucket bucket name
   * @param savePluginBeforeUnzip save plugin before unzipping
   * @param s3BackendConfig s3 backend configuration
   * @param s3 s3 client
   */
  public S3Backend(
      String serviceBucket,
      boolean savePluginBeforeUnzip,
      S3BackendConfig s3BackendConfig,
      S3Client s3) {
    this(
        serviceBucket,
        savePluginBeforeUnzip,
        s3BackendConfig,
        s3,
        Executors.newFixedThreadPool(ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS),
        ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS,
        true);
  }

  /**
   * Constructor for testing with pre-configured S3AsyncClient and TransferManager.
   *
   * @param serviceBucket bucket name
   * @param savePluginBeforeUnzip save plugin before unzipping
   * @param s3BackendConfig s3 backend configuration
   * @param s3 s3 client
   * @param s3Async s3 async client
   * @param transferManager s3 transfer manager
   */
  public S3Backend(
      String serviceBucket,
      boolean savePluginBeforeUnzip,
      S3BackendConfig s3BackendConfig,
      S3Client s3,
      S3AsyncClient s3Async,
      S3TransferManager transferManager) {
    this(
        serviceBucket,
        savePluginBeforeUnzip,
        s3BackendConfig,
        s3,
        s3Async,
        transferManager,
        Executors.newFixedThreadPool(ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS),
        ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS,
        true);
  }

  private S3Backend(
      String serviceBucket,
      boolean savePluginBeforeUnzip,
      S3BackendConfig s3BackendConfig,
      S3Client s3,
      ExecutorService executor,
      int maxExecutorParallelism,
      boolean shutdownExecutor) {
    this.s3 = s3;
    this.executor = executor;
    this.maxExecutorParallelism = maxExecutorParallelism;
    this.shutdownExecutor = shutdownExecutor;
    this.saveBeforeUnzip = savePluginBeforeUnzip;
    this.serviceBucket = serviceBucket;

    // Create S3AsyncClient for TransferManager
    // Check if serviceClientConfiguration() is available (may be null for mock objects in tests)
    if (s3.serviceClientConfiguration() != null) {
      this.s3Async =
          S3AsyncClient.crtBuilder()
              .credentialsProvider(s3.serviceClientConfiguration().credentialsProvider())
              .region(s3.serviceClientConfiguration().region())
              .build();
      this.transferManager = S3TransferManager.builder().s3Client(s3Async).build();
    } else {
      // For tests or cases where service configuration is not available
      this.s3Async = null;
      this.transferManager = null;
    }

    this.s3Metrics = s3BackendConfig.metrics;
    if (s3BackendConfig.getRateLimitBytes() > 0) {
      logger.info(
          "Enabling S3 download rate limit: {} bytes per second, window: {} seconds",
          s3BackendConfig.getRateLimitBytes(),
          s3BackendConfig.getRateLimitWindowSeconds());
      this.rateLimiter =
          new GlobalWindowRateLimiter(
              s3BackendConfig.getRateLimitBytes(), s3BackendConfig.getRateLimitWindowSeconds());
    } else {
      this.rateLimiter = null;
    }
  }

  private S3Backend(
      String serviceBucket,
      boolean savePluginBeforeUnzip,
      S3BackendConfig s3BackendConfig,
      S3Client s3,
      S3AsyncClient s3Async,
      S3TransferManager transferManager,
      ExecutorService executor,
      int maxExecutorParallelism,
      boolean shutdownExecutor) {
    this.s3 = s3;
    this.s3Async = s3Async;
    this.transferManager = transferManager;
    this.executor = executor;
    this.maxExecutorParallelism = maxExecutorParallelism;
    this.shutdownExecutor = shutdownExecutor;
    this.saveBeforeUnzip = savePluginBeforeUnzip;
    this.serviceBucket = serviceBucket;

    this.s3Metrics = s3BackendConfig.metrics;
    if (s3BackendConfig.getRateLimitBytes() > 0) {
      logger.info(
          "Enabling S3 download rate limit: {} bytes per second, window: {} seconds",
          s3BackendConfig.getRateLimitBytes(),
          s3BackendConfig.getRateLimitWindowSeconds());
      this.rateLimiter =
          new GlobalWindowRateLimiter(
              s3BackendConfig.getRateLimitBytes(), s3BackendConfig.getRateLimitWindowSeconds());
    } else {
      this.rateLimiter = null;
    }
  }

  public S3Client getS3() {
    return s3;
  }

  public S3AsyncClient getS3Async() {
    return s3Async;
  }

  public S3TransferManager getTransferManager() {
    return transferManager;
  }

  @VisibleForTesting
  ExecutorService getExecutor() {
    return executor;
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
    if (s3Path == null || !s3Path.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 URI: " + s3Path);
    }
    String withoutProtocol = s3Path.substring(5);
    int firstSlash = withoutProtocol.indexOf('/');
    String bucket = withoutProtocol.substring(0, firstSlash);
    String key = withoutProtocol.substring(firstSlash + 1);
    return downloadFromS3Path(bucket, key, true);
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
    HeadObjectRequest headRequest =
        HeadObjectRequest.builder().bucket(bucketName).key(absoluteResourcePath).build();
    long fullObjectSize;
    try {
      HeadObjectResponse headResponse = s3.headObject(headRequest);
      fullObjectSize = headResponse.contentLength();
    } catch (NoSuchKeyException e) {
      String error = String.format("Object s3://%s/%s not found", bucketName, absoluteResourcePath);
      throw new IllegalArgumentException(error, e);
    } catch (S3Exception e) {
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
    HeadObjectRequest partHeadRequest =
        HeadObjectRequest.builder()
            .bucket(bucketName)
            .key(absoluteResourcePath)
            .partNumber(1)
            .build();
    HeadObjectResponse partMetadata = s3.headObject(partHeadRequest);
    int numParts = partMetadata.partsCount() != null ? partMetadata.partsCount() : 1;
    if (verbose) {
      logger.info("Object parts: " + numParts);
    }
    s3InputStream = getObjectStream(bucketName, absoluteResourcePath, numParts);
    if (verbose) {
      logger.info("Object streaming started...");
    }
    // Wrap the stream with metrics and rate limiter if enabled
    return wrapDownloadStream(s3InputStream, s3Metrics, null, rateLimiter);
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
        while (pendingParts.size() < maxExecutorParallelism && queuedPart <= numParts) {
          // set to final variable for use in lambda
          final int finalPart = queuedPart;
          pendingParts.add(
              executor.submit(
                  () -> {
                    GetObjectRequest getRequest =
                        GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .partNumber(finalPart)
                            .build();
                    return s3.getObject(getRequest);
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
    if (transferManager != null) {
      transferManager.close();
    }
    if (s3Async != null) {
      s3Async.close();
    }
    if (shutdownExecutor) {
      try {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    s3.close();
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
    return downloadResource(prefix, null);
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
    return downloadResource(prefix, indexIdentifier);
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
    return downloadResource(prefix, indexIdentifier);
  }

  @Override
  public void uploadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException {
    List<FileNamePair> fileList = getFileNamePairs(files);
    String backendPrefix = getIndexDataPrefix(service, indexIdentifier);
    List<FileUpload> uploadList = new LinkedList<>();
    boolean hasFailure = false;
    Throwable failureCause = null;
    for (FileNamePair pair : fileList) {
      String backendKey = backendPrefix + pair.backendFileName;
      Path localFile = indexDir.resolve(pair.fileName);
      UploadFileRequest request =
          UploadFileRequest.builder()
              .putObjectRequest(
                  PutObjectRequest.builder().bucket(serviceBucket).key(backendKey).build())
              .source(localFile)
              .addTransferListener(
                  new S3ProgressListenerImpl(service, indexIdentifier, "upload_index_files"))
              .build();
      try {
        if (transferManager == null) {
          throw new IllegalStateException(
              "TransferManager is not available. Ensure S3Client is properly configured.");
        }
        FileUpload upload = transferManager.uploadFile(request);
        uploadList.add(upload);
      } catch (Throwable t) {
        hasFailure = true;
        failureCause = t;
        break;
      }
    }

    for (FileUpload upload : uploadList) {
      if (hasFailure) {
        // SDK v2 doesn't have an abort method on FileUpload, just let it complete or fail
        try {
          upload.completionFuture().cancel(true);
        } catch (Exception ignored) {
        }
      } else {
        try {
          upload.completionFuture().join();
        } catch (Throwable t) {
          hasFailure = true;
          failureCause = t;
        }
      }
    }
    if (hasFailure) {
      throw new IOException("Error while uploading index files to s3. ", failureCause);
    }
  }

  @Override
  public void downloadIndexFiles(
      String service, String indexIdentifier, Path indexDir, Map<String, NrtFileMetaData> files)
      throws IOException {
    List<FileNamePair> fileList = getFileNamePairs(files);
    String backendPrefix = getIndexDataPrefix(service, indexIdentifier);
    List<FileDownload> downloadList = new LinkedList<>();
    boolean hasFailure = false;
    Throwable failureCause = null;
    for (FileNamePair pair : fileList) {
      String backendKey = backendPrefix + pair.backendFileName;
      Path localFile = indexDir.resolve(pair.fileName);
      DownloadFileRequest request =
          DownloadFileRequest.builder()
              .getObjectRequest(
                  GetObjectRequest.builder().bucket(serviceBucket).key(backendKey).build())
              .destination(localFile)
              .addTransferListener(
                  new S3ProgressListenerImpl(service, indexIdentifier, "download_index_files"))
              .build();
      try {
        if (transferManager == null) {
          throw new IllegalStateException(
              "TransferManager is not available. Ensure S3Client is properly configured.");
        }
        FileDownload download = transferManager.downloadFile(request);
        downloadList.add(download);
      } catch (Throwable t) {
        hasFailure = true;
        failureCause = t;
        break;
      }
    }

    for (FileDownload download : downloadList) {
      if (hasFailure) {
        // SDK v2 doesn't have an abort method on FileDownload, just let it complete or fail
        try {
          download.completionFuture().cancel(true);
        } catch (Exception ignored) {
        }
      } else {
        try {
          download.completionFuture().join();
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
    return wrapDownloadStream(
        downloadFromS3Path(serviceBucket, backendKey, false),
        s3Metrics,
        indexIdentifier,
        rateLimiter);
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

  @VisibleForTesting
  static InputStream wrapDownloadStream(
      InputStream inputStream,
      boolean s3Metrics,
      String indexIdentifier,
      GlobalWindowRateLimiter rateLimiter) {
    InputStream downloadStream = inputStream;
    if (s3Metrics) {
      // Always call getBaseIndexName even if indexIdentifier is null (for test verification)
      String indexName;
      try {
        indexName = BackendGlobalState.getBaseIndexName(indexIdentifier);
      } catch (NullPointerException e) {
        // Use "unknown" as the index name when indexIdentifier is null
        indexName = "unknown";
      }
      downloadStream = new S3DownloadStreamWrapper(downloadStream, indexName);
    }
    if (rateLimiter != null) {
      downloadStream = new GlobalThrottledInputStream(downloadStream, rateLimiter);
    }
    return downloadStream;
  }

  @Override
  public void uploadPointState(
      String service, String indexIdentifier, NrtPointState nrtPointState, byte[] data)
      throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.POINT_STATE);
    String fileName = getPointStateFileName(nrtPointState);
    String backendKey = prefix + fileName;
    PutObjectRequest request =
        PutObjectRequest.builder()
            .bucket(serviceBucket)
            .key(backendKey)
            .contentLength((long) data.length)
            .build();
    s3.putObject(request, RequestBody.fromBytes(data));

    setCurrentResource(prefix, fileName);
  }

  @Override
  public InputStreamWithTimestamp downloadPointState(
      String service, String indexIdentifier, UpdateIntervalContext updateIntervalContext)
      throws IOException {
    String prefix = getIndexResourcePrefix(service, indexIdentifier, IndexResourceType.POINT_STATE);
    String fileName = getCurrentResourceName(prefix);
    String currentBackendKey = prefix + fileName;
    String currentTimeString = getTimeStringFromPointStateFileName(fileName);
    Instant currentTimestamp = TimeStringUtils.parseTimeStringSec(currentTimeString);

    BackendKeyWithTimestamp backendKeyWithTimestamp =
        getBackendKeyWithTimestamp(
            serviceBucket, prefix, currentBackendKey, currentTimestamp, updateIntervalContext);
    if (backendKeyWithTimestamp == null) {
      return null;
    }
    return new InputStreamWithTimestamp(
        wrapDownloadStream(
            downloadFromS3Path(serviceBucket, backendKeyWithTimestamp.backendKey(), true),
            s3Metrics,
            indexIdentifier,
            rateLimiter),
        backendKeyWithTimestamp.timestamp());
  }

  /**
   * Get the S3 object key for an index file and its timestamp.
   *
   * @param backendKey object key
   * @param timestamp timestamp
   */
  private record BackendKeyWithTimestamp(String backendKey, Instant timestamp) {}

  /**
   * Get the S3 object key for an index file and its timestamp, considering the update interval. If
   * the update interval is 0 or less, return the current backend key and timestamp. If the current
   * timestamp is older than the update interval, return the current backend key and timestamp.
   * Otherwise, return the first object key within the current update interval and its timestamp. If
   * the currently loaded index version is already within the update interval, return null to
   * indicate that no change is needed.
   *
   * @param bucket bucket name
   * @param prefix object prefix
   * @param currentBackendKey current object key
   * @param currentTimestamp current timestamp
   * @param updateIntervalContext update interval context, or null to get the current version
   * @return object key and timestamp
   * @throws IOException if an error occurs while listing objects
   */
  private BackendKeyWithTimestamp getBackendKeyWithTimestamp(
      String bucket,
      String prefix,
      String currentBackendKey,
      Instant currentTimestamp,
      UpdateIntervalContext updateIntervalContext)
      throws IOException {
    if (updateIntervalContext == null || updateIntervalContext.updateIntervalSeconds() <= 0) {
      return new BackendKeyWithTimestamp(currentBackendKey, currentTimestamp);
    }
    long secondsOld = Duration.between(currentTimestamp, Instant.now()).toSeconds();
    // There were no updates within the update interval, advance to the latest version
    if (secondsOld > updateIntervalContext.updateIntervalSeconds()) {
      return new BackendKeyWithTimestamp(currentBackendKey, currentTimestamp);
    }

    Instant lastIntervalStart =
        getIntervalStart(
            currentTimestamp,
            updateIntervalContext.updateIntervalSeconds(),
            updateIntervalContext.updateIntervalOffsetSeconds());
    if (updateIntervalContext.currentIndexTimestamp() != null) {
      Instant indexCurrentIntervalStart =
          getIntervalStart(
              updateIntervalContext.currentIndexTimestamp(),
              updateIntervalContext.updateIntervalSeconds(),
              updateIntervalContext.updateIntervalOffsetSeconds());
      if (!indexCurrentIntervalStart.isBefore(lastIntervalStart)) {
        // We are already at a version within the current update interval, we do not need to
        // continue
        return null;
      }
    }
    String lastIntervalTimeString = formatTimeStringSec(lastIntervalStart);
    String firstVersionKey = getFirstKeyAfter(bucket, prefix, lastIntervalTimeString);
    if (firstVersionKey == null) {
      throw new IllegalArgumentException("No version found after " + lastIntervalTimeString);
    }
    String firstVersionFileName = firstVersionKey.split(prefix)[1];
    String firstVersionTimeString = firstVersionFileName.split("-")[0];
    Instant firstVersionTimestamp = TimeStringUtils.parseTimeStringSec(firstVersionTimeString);
    return new BackendKeyWithTimestamp(firstVersionKey, firstVersionTimestamp);
  }

  /**
   * Get the timestamp for the start of the interval containing the specified timestamp. Intervals
   * start at midnight UTC for each day.
   *
   * @param timestamp timestamp
   * @param intervalSeconds interval in seconds
   * @param intervalOffsetSeconds interval offset in seconds
   * @return start of the interval containing the specified timestamp
   */
  @VisibleForTesting
  static Instant getIntervalStart(
      Instant timestamp, int intervalSeconds, int intervalOffsetSeconds) {
    LocalTime localTime = timestamp.atZone(ZoneId.of("UTC")).toLocalTime();
    int secondsSinceMidnight = localTime.toSecondOfDay();
    int adjustedSeconds = adjustForOffset(secondsSinceMidnight, intervalOffsetSeconds);
    int remainderSeconds = adjustedSeconds % intervalSeconds;
    return timestamp.minusSeconds(remainderSeconds);
  }

  /**
   * Adjust seconds since midnight by the specified offset, wrapping around at midnight if needed.
   *
   * @param secondsSinceMidnight seconds since midnight
   * @param offsetSeconds offset in seconds
   * @return adjusted seconds since midnight
   */
  @VisibleForTesting
  static int adjustForOffset(int secondsSinceMidnight, int offsetSeconds) {
    int adjusted = secondsSinceMidnight - offsetSeconds;
    if (adjusted < 0) {
      adjusted += SECONDS_PER_DAY;
    }
    return adjusted;
  }

  /**
   * Get the first object key after the specified time string. Returns null if no such object
   * exists.
   *
   * @param bucket bucket name
   * @param prefix object prefix
   * @param timeString time string
   * @return first object key after the specified time string, or null if no such object exists
   * @throws IOException if an error occurs while listing objects
   */
  private String getFirstKeyAfter(String bucket, String prefix, String timeString)
      throws IOException {
    ListObjectsV2Request req =
        ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(prefix)
            .startAfter(prefix + timeString)
            .maxKeys(1)
            .build();
    ListObjectsV2Response result = s3.listObjectsV2(req);
    if (result.keyCount() == 0) {
      return null;
    }
    String objectKey = result.contents().get(0).key();
    if (objectKey.endsWith(CURRENT_VERSION)) {
      return null;
    }
    return objectKey;
  }

  private void uploadResource(
      String prefix, String fileName, String service, String progressIdentifier, byte[] data)
      throws IOException {
    String backendKey = prefix + fileName;
    PutObjectRequest request =
        PutObjectRequest.builder()
            .bucket(serviceBucket)
            .key(backendKey)
            .contentLength((long) data.length)
            .build();
    s3.putObject(request, RequestBody.fromBytes(data));

    setCurrentResource(prefix, fileName);
  }

  private InputStream downloadResource(String prefix, String indexIdentifier) throws IOException {
    String fileName = getCurrentResourceName(prefix);
    String backendKey = prefix + fileName;
    InputStream inputStream = downloadFromS3Path(serviceBucket, backendKey, true);
    if (indexIdentifier != null) {
      inputStream = wrapDownloadStream(inputStream, s3Metrics, indexIdentifier, rateLimiter);
    }
    return inputStream;
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
      GetObjectRequest request = GetObjectRequest.builder().bucket(serviceBucket).key(key).build();
      ResponseInputStream<GetObjectResponse> responseStream = s3.getObject(request);
      byte[] versionBytes = IOUtils.toByteArray(responseStream);
      return StateUtils.fromUTF8(versionBytes);
    } catch (NoSuchKeyException e) {
      String error = String.format("Object s3://%s/%s not found", serviceBucket, key);
      throw new IllegalArgumentException(error, e);
    } catch (S3Exception e) {
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
    PutObjectRequest request =
        PutObjectRequest.builder()
            .bucket(serviceBucket)
            .key(key)
            .contentLength((long) bytes.length)
            .build();
    s3.putObject(request, RequestBody.fromBytes(bytes));
    logger.info("Set current resource - prefix: {}, version: {}", prefix, version);
  }

  boolean currentResourceExists(String prefix) {
    String key = prefix + CURRENT_VERSION;
    try {
      HeadObjectRequest request =
          HeadObjectRequest.builder().bucket(serviceBucket).key(key).build();
      s3.headObject(request);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      if (isNotFoundException(e)) {
        return false;
      }
      throw e;
    }
  }

  private boolean isNotFoundException(S3Exception e) {
    return e.statusCode() == 404;
  }
}
