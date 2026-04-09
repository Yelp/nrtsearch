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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.services.s3.multipart.ParallelConfiguration;

/** Utility class for working with S3. */
public class S3Util {
  private static final Logger logger = LoggerFactory.getLogger(S3Util.class);

  private S3Util() {}

  /**
   * Container for S3 clients.
   *
   * @param s3Client synchronous S3 client
   * @param s3AsyncClient asynchronous S3 client
   */
  public record S3ClientBundle(S3Client s3Client, S3AsyncClient s3AsyncClient) {}

  /** Configuration for the S3 CRT async client. */
  public static class S3CrtConfig {
    private static final String CONFIG_PREFIX = "remoteConfig.s3.crt.";

    private final double targetThroughputInGbps;
    private final int maxConcurrency;
    private final long minimumPartSizeInBytes;
    private final long maxNativeMemoryLimitInBytes;

    /**
     * Create S3CrtConfig from NrtsearchConfig.
     *
     * <p>At most one of {@code targetThroughputInGbps} or {@code maxConcurrency} may be set (> 0).
     * If neither is configured, the SDK default is used for both (no value set on the builder).
     *
     * @param configuration server configuration
     * @return S3CrtConfig
     */
    public static S3CrtConfig fromConfig(NrtsearchConfig configuration) {
      double targetThroughputInGbps =
          configuration.getConfigReader().getDouble(CONFIG_PREFIX + "targetThroughputInGbps", 0.0);
      int maxConcurrency =
          configuration.getConfigReader().getInteger(CONFIG_PREFIX + "maxConcurrency", 0);
      long minimumPartSizeInBytes =
          sizeStrToBytes(
              configuration.getConfigReader().getString(CONFIG_PREFIX + "minimumPartSize", "8mb"));
      long maxNativeMemoryLimitInBytes =
          sizeStrToBytes(
              configuration
                  .getConfigReader()
                  .getString(CONFIG_PREFIX + "maxNativeMemoryLimit", "0"));
      return new S3CrtConfig(
          targetThroughputInGbps,
          maxConcurrency,
          minimumPartSizeInBytes,
          maxNativeMemoryLimitInBytes);
    }

    /**
     * Constructor.
     *
     * <p>At most one of {@code targetThroughputInGbps} or {@code maxConcurrency} may be set (> 0);
     * setting both is not allowed because they are mutually exclusive ways to bound CRT resource
     * usage. {@code targetThroughputInGbps} lets CRT auto-calculate connections and size its
     * buffers for the target throughput. {@code maxConcurrency} hard-caps the connection count and
     * lets CRT derive buffer sizing from that cap instead. If neither is set (both 0), the SDK
     * default is used.
     *
     * @param targetThroughputInGbps target throughput in Gbps for CRT buffer/connection sizing (0
     *     to use SDK default or maxConcurrency instead)
     * @param maxConcurrency hard cap on concurrent S3 connections (0 to use SDK default or
     *     targetThroughputInGbps instead)
     * @param minimumPartSizeInBytes minimum multipart part size in bytes
     * @param maxNativeMemoryLimitInBytes hard cap on CRT native memory in bytes (0 means unlimited)
     * @throws IllegalArgumentException if both targetThroughputInGbps and maxConcurrency are set,
     *     or if minimumPartSizeInBytes <= 0
     */
    public S3CrtConfig(
        double targetThroughputInGbps,
        int maxConcurrency,
        long minimumPartSizeInBytes,
        long maxNativeMemoryLimitInBytes) {
      if (targetThroughputInGbps > 0 && maxConcurrency > 0) {
        throw new IllegalArgumentException(
            "Only one of targetThroughputInGbps or maxConcurrency may be set");
      }
      if (minimumPartSizeInBytes <= 0) {
        throw new IllegalArgumentException("minimumPartSizeInBytes must be > 0");
      }
      if (maxNativeMemoryLimitInBytes < 0) {
        throw new IllegalArgumentException("maxNativeMemoryLimitInBytes must be >= 0");
      }
      this.targetThroughputInGbps = targetThroughputInGbps;
      this.maxConcurrency = maxConcurrency;
      this.minimumPartSizeInBytes = minimumPartSizeInBytes;
      this.maxNativeMemoryLimitInBytes = maxNativeMemoryLimitInBytes;
    }

    /**
     * Get the target throughput in Gbps (0 means not set; SDK default will be used).
     *
     * @return target throughput in Gbps
     */
    public double getTargetThroughputInGbps() {
      return targetThroughputInGbps;
    }

    /**
     * Get the max concurrency (0 means not set; SDK default will be used).
     *
     * @return max number of concurrent S3 connections
     */
    public int getMaxConcurrency() {
      return maxConcurrency;
    }

    /**
     * Get the minimum part size in bytes.
     *
     * @return minimum part size in bytes
     */
    public long getMinimumPartSizeInBytes() {
      return minimumPartSizeInBytes;
    }

    /**
     * Get the max native memory limit in bytes.
     *
     * @return max native memory limit in bytes (0 means unlimited)
     */
    public long getMaxNativeMemoryLimitInBytes() {
      return maxNativeMemoryLimitInBytes;
    }

    /**
     * Convert a size string to bytes. Supports kb, mb, gb suffixes (case-insensitive) and plain
     * numeric values (treated as bytes). For example: '8mb', '512kb', '2gb', '1048576'.
     */
    @VisibleForTesting
    static long sizeStrToBytes(String sizeStr) {
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
            multiplier = 1024L * 1024 * 1024;
          }
        }
      }
      if (baseStr.isEmpty()) {
        throw new IllegalArgumentException("Cannot convert size string: " + sizeStr);
      }
      double baseValue = Double.parseDouble(baseStr);
      return (long) (baseValue * multiplier);
    }
  }

  /** Configuration for the Java-based S3 async client. */
  public static class S3JavaAsyncConfig {
    private static final String CONFIG_PREFIX = "remoteConfig.s3.java.";

    private final long minimumPartSizeInBytes;
    private final long thresholdSizeInBytes;
    private final long apiCallBufferSizeInBytes;
    private final int maxInFlightParts;
    private final int ioThreads;
    private final int maxConnections;
    private final int connectionTimeoutMs;
    private final int connectionAcquisitionTimeoutMs;
    private final int maxPendingConnectionAcquires;

    /**
     * Create S3JavaAsyncConfig from NrtsearchConfig.
     *
     * @param configuration server configuration
     * @return S3JavaAsyncConfig
     */
    public static S3JavaAsyncConfig fromConfig(NrtsearchConfig configuration) {
      long minimumPartSizeInBytes =
          S3CrtConfig.sizeStrToBytes(
              configuration.getConfigReader().getString(CONFIG_PREFIX + "minimumPartSize", "8mb"));
      long thresholdSizeInBytes =
          S3CrtConfig.sizeStrToBytes(
              configuration.getConfigReader().getString(CONFIG_PREFIX + "thresholdSize", "8mb"));
      long apiCallBufferSizeInBytes =
          S3CrtConfig.sizeStrToBytes(
              configuration.getConfigReader().getString(CONFIG_PREFIX + "apiCallBufferSize", "0"));
      int maxInFlightParts =
          configuration.getConfigReader().getInteger(CONFIG_PREFIX + "maxInFlightParts", 0);
      int ioThreads = configuration.getConfigReader().getInteger(CONFIG_PREFIX + "ioThreads", 0);
      int maxConnections =
          configuration.getConfigReader().getInteger(CONFIG_PREFIX + "maxConnections", 0);
      int connectionTimeoutMs =
          configuration.getConfigReader().getInteger(CONFIG_PREFIX + "connectionTimeoutMs", 0);
      int connectionAcquisitionTimeoutMs =
          configuration
              .getConfigReader()
              .getInteger(CONFIG_PREFIX + "connectionAcquisitionTimeoutMs", 0);
      int maxPendingConnectionAcquires =
          configuration
              .getConfigReader()
              .getInteger(CONFIG_PREFIX + "maxPendingConnectionAcquires", 0);
      return new S3JavaAsyncConfig(
          minimumPartSizeInBytes,
          thresholdSizeInBytes,
          apiCallBufferSizeInBytes,
          maxInFlightParts,
          ioThreads,
          maxConnections,
          connectionTimeoutMs,
          connectionAcquisitionTimeoutMs,
          maxPendingConnectionAcquires);
    }

    /**
     * Constructor.
     *
     * @param minimumPartSizeInBytes minimum multipart part size in bytes
     * @param thresholdSizeInBytes size threshold to trigger multipart upload in bytes
     * @param apiCallBufferSizeInBytes API call buffer size in bytes (0 means SDK default)
     * @param maxInFlightParts max in-flight multipart parts (0 means SDK default)
     * @param ioThreads number of Netty I/O threads (0 means SDK default)
     * @param maxConnections max connections in the Netty connection pool (0 means SDK default of
     *     50)
     * @param connectionTimeoutMs TCP connection timeout in milliseconds (0 means SDK default of
     *     2000)
     * @param connectionAcquisitionTimeoutMs timeout to acquire a connection from the pool in
     *     milliseconds (0 means SDK default of 10000)
     * @param maxPendingConnectionAcquires max requests queued waiting for a connection (0 means SDK
     *     default of 10000)
     * @throws IllegalArgumentException if minimumPartSizeInBytes or thresholdSizeInBytes are <= 0,
     *     or if any other parameter is < 0
     */
    public S3JavaAsyncConfig(
        long minimumPartSizeInBytes,
        long thresholdSizeInBytes,
        long apiCallBufferSizeInBytes,
        int maxInFlightParts,
        int ioThreads,
        int maxConnections,
        int connectionTimeoutMs,
        int connectionAcquisitionTimeoutMs,
        int maxPendingConnectionAcquires) {
      if (minimumPartSizeInBytes <= 0) {
        throw new IllegalArgumentException("minimumPartSizeInBytes must be > 0");
      }
      if (thresholdSizeInBytes <= 0) {
        throw new IllegalArgumentException("thresholdSizeInBytes must be > 0");
      }
      if (apiCallBufferSizeInBytes < 0) {
        throw new IllegalArgumentException("apiCallBufferSizeInBytes must be >= 0");
      }
      if (maxInFlightParts < 0) {
        throw new IllegalArgumentException("maxInFlightParts must be >= 0");
      }
      if (ioThreads < 0) {
        throw new IllegalArgumentException("ioThreads must be >= 0");
      }
      if (maxConnections < 0) {
        throw new IllegalArgumentException("maxConnections must be >= 0");
      }
      if (connectionTimeoutMs < 0) {
        throw new IllegalArgumentException("connectionTimeoutMs must be >= 0");
      }
      if (connectionAcquisitionTimeoutMs < 0) {
        throw new IllegalArgumentException("connectionAcquisitionTimeoutMs must be >= 0");
      }
      if (maxPendingConnectionAcquires < 0) {
        throw new IllegalArgumentException("maxPendingConnectionAcquires must be >= 0");
      }
      this.minimumPartSizeInBytes = minimumPartSizeInBytes;
      this.thresholdSizeInBytes = thresholdSizeInBytes;
      this.apiCallBufferSizeInBytes = apiCallBufferSizeInBytes;
      this.maxInFlightParts = maxInFlightParts;
      this.ioThreads = ioThreads;
      this.maxConnections = maxConnections;
      this.connectionTimeoutMs = connectionTimeoutMs;
      this.connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs;
      this.maxPendingConnectionAcquires = maxPendingConnectionAcquires;
    }

    /**
     * Get the minimum part size in bytes.
     *
     * @return minimum part size in bytes
     */
    public long getMinimumPartSizeInBytes() {
      return minimumPartSizeInBytes;
    }

    /**
     * Get the threshold size in bytes to trigger multipart upload.
     *
     * @return threshold size in bytes
     */
    public long getThresholdSizeInBytes() {
      return thresholdSizeInBytes;
    }

    /**
     * Get the API call buffer size in bytes (0 means SDK default).
     *
     * @return API call buffer size in bytes
     */
    public long getApiCallBufferSizeInBytes() {
      return apiCallBufferSizeInBytes;
    }

    /**
     * Get the max in-flight multipart parts (0 means SDK default).
     *
     * @return max in-flight multipart parts
     */
    public int getMaxInFlightParts() {
      return maxInFlightParts;
    }

    /**
     * Get the number of Netty I/O threads (0 means SDK default).
     *
     * @return number of I/O threads
     */
    public int getIoThreads() {
      return ioThreads;
    }

    /**
     * Get the max connections in the Netty connection pool (0 means SDK default of 50).
     *
     * @return max connections
     */
    public int getMaxConnections() {
      return maxConnections;
    }

    /**
     * Get the TCP connection timeout in milliseconds (0 means SDK default of 2000).
     *
     * @return connection timeout in milliseconds
     */
    public int getConnectionTimeoutMs() {
      return connectionTimeoutMs;
    }

    /**
     * Get the timeout to acquire a connection from the pool in milliseconds (0 means SDK default of
     * 10000).
     *
     * @return connection acquisition timeout in milliseconds
     */
    public int getConnectionAcquisitionTimeoutMs() {
      return connectionAcquisitionTimeoutMs;
    }

    /**
     * Get the max requests queued waiting for a connection (0 means SDK default of 10000).
     *
     * @return max pending connection acquires
     */
    public int getMaxPendingConnectionAcquires() {
      return maxPendingConnectionAcquires;
    }
  }

  /**
   * Create a new S3 client bundle from the given configuration.
   *
   * @param configuration server configuration
   * @return S3ClientBundle containing sync and async clients
   */
  private static AwsCredentialsProvider createCredentialsProvider(NrtsearchConfig configuration) {
    if (configuration.getBotoCfgPath() == null) {
      return DefaultCredentialsProvider.builder().build();
    } else {
      return ProfileCredentialsProvider.builder()
          .profileFile(
              software.amazon.awssdk.profiles.ProfileFile.builder()
                  .content(Paths.get(configuration.getBotoCfgPath()))
                  .type(software.amazon.awssdk.profiles.ProfileFile.Type.CREDENTIALS)
                  .build())
          .profileName("default")
          .build();
    }
  }

  public static S3ClientBundle buildS3ClientBundle(NrtsearchConfig configuration) {
    AwsCredentialsProvider awsCredentialsProvider = createCredentialsProvider(configuration);
    final boolean globalBucketAccess = configuration.getEnableGlobalBucketAccess();

    software.amazon.awssdk.services.s3.S3ClientBuilder clientBuilder =
        S3Client.builder().credentialsProvider(awsCredentialsProvider);

    if (globalBucketAccess) {
      clientBuilder.crossRegionAccessEnabled(true);
    }

    try {
      S3Client s3ClientInterim =
          S3Client.builder()
              .credentialsProvider(createCredentialsProvider(configuration))
              .crossRegionAccessEnabled(globalBucketAccess)
              .build();
      GetBucketLocationResponse locationResponse =
          s3ClientInterim.getBucketLocation(
              GetBucketLocationRequest.builder().bucket(configuration.getBucketName()).build());
      String regionString = locationResponse.locationConstraintAsString();
      // In useast-1, the region is returned as null or empty which is equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
      if (regionString == null || regionString.isEmpty() || regionString.equals("US")) {
        regionString = "us-east-1";
      }
      Region region = Region.of(regionString);
      logger.info(String.format("S3 region: %s", regionString));
      clientBuilder.region(region);
      s3ClientInterim.close();
    } catch (SdkException sdkClientException) {
      logger.warn(
          "failed to get the location of S3 bucket: "
              + configuration.getBucketName()
              + ". This could be caused by missing credentials and/or regions, or wrong bucket name.",
          sdkClientException);
      logger.info("return a dummy S3Client.");
      S3Client dummyClient =
          S3Client.builder()
              .credentialsProvider(AnonymousCredentialsProvider.create())
              .region(Region.US_EAST_1)
              .endpointOverride(URI.create("https://dummyService"))
              .build();
      return new S3ClientBundle(dummyClient, null);
    }

    int maxRetries = configuration.getMaxS3ClientRetries();
    software.amazon.awssdk.core.client.config.ClientOverrideConfiguration overrideConfig = null;
    if (maxRetries > 0) {
      RetryPolicy retryPolicy =
          RetryPolicy.builder(RetryMode.STANDARD).numRetries(maxRetries).build();
      overrideConfig =
          software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder()
              .retryPolicy(retryPolicy)
              .build();
      clientBuilder.overrideConfiguration(overrideConfig);
    }

    S3Client s3Client = clientBuilder.build();

    String asyncClientType =
        configuration.getConfigReader().getString("remoteConfig.s3.asyncClientType", "crt");
    S3AsyncClient s3AsyncClient;
    if (asyncClientType.equalsIgnoreCase("java")) {
      s3AsyncClient =
          buildJavaAsyncClient(configuration, s3Client, overrideConfig, globalBucketAccess);
    } else if (asyncClientType.equalsIgnoreCase("crt")) {
      s3AsyncClient = buildCrtAsyncClient(configuration, s3Client, globalBucketAccess);
    } else {
      throw new IllegalArgumentException(
          "Unknown asyncClientType: '" + asyncClientType + "'. Valid values are 'crt' or 'java'.");
    }
    return new S3ClientBundle(s3Client, s3AsyncClient);
  }

  private static S3AsyncClient buildJavaAsyncClient(
      NrtsearchConfig configuration,
      S3Client s3Client,
      software.amazon.awssdk.core.client.config.ClientOverrideConfiguration overrideConfig,
      boolean globalBucketAccess) {
    S3JavaAsyncConfig javaConfig = S3JavaAsyncConfig.fromConfig(configuration);
    logger.info(
        "S3 Java async client config: minimumPartSizeInBytes={}, thresholdSizeInBytes={}, apiCallBufferSizeInBytes={}, maxInFlightParts={}, ioThreads={}, maxConnections={}, connectionTimeoutMs={}, connectionAcquisitionTimeoutMs={}, maxPendingConnectionAcquires={}",
        javaConfig.getMinimumPartSizeInBytes(),
        javaConfig.getThresholdSizeInBytes(),
        javaConfig.getApiCallBufferSizeInBytes(),
        javaConfig.getMaxInFlightParts(),
        javaConfig.getIoThreads(),
        javaConfig.getMaxConnections(),
        javaConfig.getConnectionTimeoutMs(),
        javaConfig.getConnectionAcquisitionTimeoutMs(),
        javaConfig.getMaxPendingConnectionAcquires());

    MultipartConfiguration.Builder multipartBuilder =
        MultipartConfiguration.builder()
            .minimumPartSizeInBytes(javaConfig.getMinimumPartSizeInBytes())
            .thresholdInBytes(javaConfig.getThresholdSizeInBytes());
    if (javaConfig.getApiCallBufferSizeInBytes() > 0) {
      multipartBuilder.apiCallBufferSizeInBytes(javaConfig.getApiCallBufferSizeInBytes());
    }
    if (javaConfig.getMaxInFlightParts() > 0) {
      multipartBuilder.parallelConfiguration(
          ParallelConfiguration.builder()
              .maxInFlightParts(javaConfig.getMaxInFlightParts())
              .build());
    }

    software.amazon.awssdk.services.s3.S3AsyncClientBuilder builder =
        S3AsyncClient.builder()
            .credentialsProvider(createCredentialsProvider(configuration))
            .region(s3Client.serviceClientConfiguration().region())
            .multipartEnabled(true)
            .multipartConfiguration(multipartBuilder.build());
    if (javaConfig.getIoThreads() > 0
        || javaConfig.getMaxConnections() > 0
        || javaConfig.getConnectionTimeoutMs() > 0
        || javaConfig.getConnectionAcquisitionTimeoutMs() > 0
        || javaConfig.getMaxPendingConnectionAcquires() > 0) {
      NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder();
      if (javaConfig.getIoThreads() > 0) {
        nettyBuilder.eventLoopGroupBuilder(
            SdkEventLoopGroup.builder().numberOfThreads(javaConfig.getIoThreads()));
      }
      if (javaConfig.getMaxConnections() > 0) {
        nettyBuilder.maxConcurrency(javaConfig.getMaxConnections());
      }
      if (javaConfig.getConnectionTimeoutMs() > 0) {
        nettyBuilder.connectionTimeout(Duration.ofMillis(javaConfig.getConnectionTimeoutMs()));
      }
      if (javaConfig.getConnectionAcquisitionTimeoutMs() > 0) {
        nettyBuilder.connectionAcquisitionTimeout(
            Duration.ofMillis(javaConfig.getConnectionAcquisitionTimeoutMs()));
      }
      if (javaConfig.getMaxPendingConnectionAcquires() > 0) {
        nettyBuilder.maxPendingConnectionAcquires(javaConfig.getMaxPendingConnectionAcquires());
      }
      builder.httpClientBuilder(nettyBuilder);
    }
    if (overrideConfig != null) {
      builder.overrideConfiguration(overrideConfig);
    }
    if (globalBucketAccess) {
      builder.crossRegionAccessEnabled(true);
    }
    return builder.build();
  }

  private static S3AsyncClient buildCrtAsyncClient(
      NrtsearchConfig configuration, S3Client s3Client, boolean globalBucketAccess) {
    S3CrtConfig crtConfig = S3CrtConfig.fromConfig(configuration);
    logger.info(
        "S3 CRT client config: targetThroughputInGbps={}, maxConcurrency={}, minimumPartSizeInBytes={}, maxNativeMemoryLimitInBytes={}",
        crtConfig.getTargetThroughputInGbps(),
        crtConfig.getMaxConcurrency(),
        crtConfig.getMinimumPartSizeInBytes(),
        crtConfig.getMaxNativeMemoryLimitInBytes());

    S3CrtAsyncClientBuilder builder =
        S3CrtAsyncClient.builder()
            .credentialsProvider(createCredentialsProvider(configuration))
            .region(s3Client.serviceClientConfiguration().region())
            .minimumPartSizeInBytes(crtConfig.getMinimumPartSizeInBytes());

    if (crtConfig.getTargetThroughputInGbps() > 0) {
      builder.targetThroughputInGbps(crtConfig.getTargetThroughputInGbps());
    } else if (crtConfig.getMaxConcurrency() > 0) {
      builder.maxConcurrency(crtConfig.getMaxConcurrency());
    }
    if (crtConfig.getMaxNativeMemoryLimitInBytes() > 0) {
      builder.maxNativeMemoryLimitInBytes(crtConfig.getMaxNativeMemoryLimitInBytes());
    }
    if (globalBucketAccess) {
      builder.crossRegionAccessEnabled(true);
    }
    return builder.build();
  }

  /**
   * Parsed S3 URI components.
   *
   * @param bucket S3 bucket name
   * @param key S3 object key
   */
  public record S3UriComponents(String bucket, String key) {}

  /**
   * Parse an S3 URI path into bucket and key components.
   *
   * <p>This method replaces the old AmazonS3URI usage from AWS SDK v1.
   *
   * @param s3Path Complete S3 URI path (e.g., s3://bucket-name/path/to/object)
   * @return S3UriComponents containing bucket and key
   * @throws IllegalArgumentException if the path is not a valid S3 URI
   */
  public static S3UriComponents parseS3Uri(String s3Path) {
    if (s3Path == null || !s3Path.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 URI: " + s3Path);
    }
    String withoutProtocol = s3Path.substring(5);
    int firstSlash = withoutProtocol.indexOf('/');
    if (firstSlash < 0) {
      // No key, just bucket
      return new S3UriComponents(withoutProtocol, "");
    }
    String bucket = withoutProtocol.substring(0, firstSlash);
    String key = withoutProtocol.substring(firstSlash + 1);
    return new S3UriComponents(bucket, key);
  }

  /**
   * Parse S3 URI and extract the key.
   *
   * @param path S3 URI path (e.g., s3://bucket/key)
   * @return S3 key
   */
  private static String parseS3Key(String path) {
    return parseS3Uri(path).key();
  }

  /**
   * Check if a string is a valid S3 file path.
   *
   * @param path String to check
   * @return True if the string is a valid S3 file path, false otherwise
   */
  public static boolean isValidS3FilePath(String path) {
    try {
      String key = parseS3Key(path);
      return isKeyValidForFile(key);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Get name of file in S3 from the S3 path, assuming that the name is the last part of a key with
   * directories separated by "/".
   *
   * @param path S3 path
   * @return Name of file
   */
  public static String getS3FileName(String path) {
    String key = parseS3Key(path);
    if (!isKeyValidForFile(key)) {
      throw new IllegalArgumentException(String.format("S3 path %s is not valid for a file", path));
    }
    String[] split = key.split("/");
    return split[split.length - 1];
  }

  private static boolean isKeyValidForFile(String key) {
    return !Strings.isNullOrEmpty(key) && !key.endsWith("/");
  }

  /**
   * Check if a key exists in S3.
   *
   * @param s3Client S3 client
   * @param bucket S3 bucket name
   * @param key S3 object key
   * @return true if the key exists, false otherwise
   */
  public static boolean doesKeyExist(S3Client s3Client, String bucket, String key) {
    try {
      HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
      s3Client.headObject(request);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return false;
      }
      throw e;
    }
  }
}
