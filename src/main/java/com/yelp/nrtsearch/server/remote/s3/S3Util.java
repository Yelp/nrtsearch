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
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;

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

  /**
   * Create a new S3 client bundle from the given configuration.
   *
   * @param configuration server configuration
   * @return S3ClientBundle containing sync and async clients
   */
  public static S3ClientBundle buildS3ClientBundle(NrtsearchConfig configuration) {
    AwsCredentialsProvider awsCredentialsProvider;
    if (configuration.getBotoCfgPath() == null) {
      awsCredentialsProvider = DefaultCredentialsProvider.create();
    } else {
      awsCredentialsProvider =
          ProfileCredentialsProvider.builder()
              .profileFile(
                  software.amazon.awssdk.profiles.ProfileFile.builder()
                      .content(Paths.get(configuration.getBotoCfgPath()))
                      .type(software.amazon.awssdk.profiles.ProfileFile.Type.CREDENTIALS)
                      .build())
              .profileName("default")
              .build();
    }
    final boolean globalBucketAccess = configuration.getEnableGlobalBucketAccess();

    software.amazon.awssdk.services.s3.S3ClientBuilder clientBuilder =
        S3Client.builder().credentialsProvider(awsCredentialsProvider);

    if (globalBucketAccess) {
      clientBuilder.crossRegionAccessEnabled(true);
    }

    try {
      S3Client s3ClientInterim =
          S3Client.builder()
              .credentialsProvider(awsCredentialsProvider)
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
      String serviceEndpoint = String.format("https://s3.%s.amazonaws.com", regionString);
      logger.info(String.format("S3 ServiceEndpoint: %s", serviceEndpoint));
      clientBuilder.region(region).endpointOverride(URI.create(serviceEndpoint));
      s3ClientInterim.close();
    } catch (SdkClientException sdkClientException) {
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
    S3AsyncClient s3AsyncClient =
        S3AsyncClient.crtBuilder()
            .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
            .region(s3Client.serviceClientConfiguration().region())
            .build();
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
}
