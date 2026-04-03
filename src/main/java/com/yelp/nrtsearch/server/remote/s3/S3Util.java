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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
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
    if (maxRetries > 0) {
      RetryPolicy retryPolicy =
          RetryPolicy.builder(RetryMode.STANDARD).numRetries(maxRetries).build();
      software.amazon.awssdk.core.client.config.ClientOverrideConfiguration overrideConfig =
          software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder()
              .retryPolicy(retryPolicy)
              .build();
      clientBuilder.overrideConfiguration(overrideConfig);
    }

    S3Client s3Client = clientBuilder.build();

    S3CrtConfig crtConfig = S3CrtConfig.fromConfig(configuration);
    logger.info(
        "S3 CRT client config: targetThroughputInGbps={}, maxConcurrency={}, minimumPartSizeInBytes={}, maxNativeMemoryLimitInBytes={}",
        crtConfig.getTargetThroughputInGbps(),
        crtConfig.getMaxConcurrency(),
        crtConfig.getMinimumPartSizeInBytes(),
        crtConfig.getMaxNativeMemoryLimitInBytes());

    S3CrtAsyncClientBuilder s3CrtAsyncClientBuilder =
        S3CrtAsyncClient.builder()
            .credentialsProvider(createCredentialsProvider(configuration))
            .region(s3Client.serviceClientConfiguration().region())
            .minimumPartSizeInBytes(crtConfig.getMinimumPartSizeInBytes());

    if (crtConfig.getTargetThroughputInGbps() > 0) {
      s3CrtAsyncClientBuilder.targetThroughputInGbps(crtConfig.getTargetThroughputInGbps());
    } else if (crtConfig.getMaxConcurrency() > 0) {
      s3CrtAsyncClientBuilder.maxConcurrency(crtConfig.getMaxConcurrency());
    }

    if (crtConfig.getMaxNativeMemoryLimitInBytes() > 0) {
      s3CrtAsyncClientBuilder.maxNativeMemoryLimitInBytes(
          crtConfig.getMaxNativeMemoryLimitInBytes());
    }

    if (globalBucketAccess) {
      s3CrtAsyncClientBuilder.crossRegionAccessEnabled(true);
    }

    S3AsyncClient s3AsyncClient = s3CrtAsyncClientBuilder.build();
    return new S3ClientBundle(s3Client, s3AsyncClient);
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
