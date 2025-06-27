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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.base.Strings;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for working with S3. */
public class S3Util {
  private static final Logger logger = LoggerFactory.getLogger(S3Util.class);

  private S3Util() {}

  /**
   * Create a new s3 client from the given configuration.
   *
   * @param configuration server configuration
   * @return s3 client
   */
  public static AmazonS3 buildS3Client(NrtsearchConfig configuration) {
    AWSCredentialsProvider awsCredentialsProvider;
    if (configuration.getBotoCfgPath() == null) {
      awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
    } else {
      Path botoCfgPath = Paths.get(configuration.getBotoCfgPath());
      final ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
      awsCredentialsProvider = new ProfileCredentialsProvider(profilesConfigFile, "default");
    }
    final boolean globalBucketAccess = configuration.getEnableGlobalBucketAccess();

    AmazonS3ClientBuilder clientBuilder =
        AmazonS3ClientBuilder.standard()
            .withCredentials(awsCredentialsProvider)
            .withForceGlobalBucketAccessEnabled(globalBucketAccess);
    try {
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard()
              .withCredentials(awsCredentialsProvider)
              .withForceGlobalBucketAccessEnabled(globalBucketAccess)
              .build();
      String region = s3ClientInterim.getBucketLocation(configuration.getBucketName());
      // In useast-1, the region is returned as "US" which is an equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html#US_Standard
      // However, this causes an UnknownHostException so we override it to the full region name
      if (region.equals("US")) {
        region = "us-east-1";
      }
      String serviceEndpoint = String.format("s3.%s.amazonaws.com", region);
      logger.info("S3 ServiceEndpoint: {}", serviceEndpoint);
      clientBuilder.withEndpointConfiguration(new EndpointConfiguration(serviceEndpoint, region));
    } catch (SdkClientException sdkClientException) {
      logger.warn("failed to get the location of S3 bucket: {}. This could be caused by missing credentials and/or regions, or wrong bucket name.", configuration.getBucketName(), sdkClientException);
      logger.info("return a dummy AmazonS3.");
      return AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration("dummyService", "dummyRegion"))
          .build();
    }

    int maxRetries = configuration.getMaxS3ClientRetries();
    if (maxRetries > 0) {
      RetryPolicy retryPolicy =
          new RetryPolicy(
              PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
              PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
              maxRetries,
              true);
      ClientConfiguration clientConfiguration =
          new ClientConfiguration().withRetryPolicy(retryPolicy);
      clientBuilder.setClientConfiguration(clientConfiguration);
    }

    return clientBuilder.build();
  }

  /**
   * Check if a string is a valid S3 file path.
   *
   * @param path String to check
   * @return True if the string is a valid S3 file path, false otherwise
   */
  public static boolean isValidS3FilePath(String path) {
    try {
      AmazonS3URI s3URI = new AmazonS3URI(path);
      String key = s3URI.getKey();
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
    AmazonS3URI s3URI = new AmazonS3URI(path);
    String key = s3URI.getKey();
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
