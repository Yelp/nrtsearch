/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.state.StateUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;

public class StateCommandUtils {
  public static final String GLOBAL_STATE_RESOURCE = "global_state";
  public static final String NOT_SET = "not_set";

  private StateCommandUtils() {}

  /**
   * Check if the given resource name is for global state.
   *
   * @param resourceName resource name
   * @return true if global state
   */
  public static boolean isGlobalState(String resourceName) {
    return GLOBAL_STATE_RESOURCE.equals(resourceName);
  }

  /**
   * Get an S3 client usable for remote state operations.
   *
   * @param bucketName s3 bucket
   * @param region aws region, such as us-west-2, or null to detect
   * @param credsFile file containing aws credentials, or null for default
   * @param credsProfile profile to use from credentials file
   * @return s3 client
   */
  public static S3Client createS3Client(
      String bucketName, String region, String credsFile, String credsProfile, int maxRetry) {
    AwsCredentialsProvider awsCredentialsProvider;
    if (credsFile != null) {
      awsCredentialsProvider =
          ProfileCredentialsProvider.builder()
              .profileFile(
                  software.amazon.awssdk.profiles.ProfileFile.builder()
                      .content(Paths.get(credsFile))
                      .type(software.amazon.awssdk.profiles.ProfileFile.Type.CREDENTIALS)
                      .build())
              .profileName(credsProfile)
              .build();
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.create();
    }

    String clientRegion;
    if (region == null) {
      S3Client s3ClientInterim =
          S3Client.builder().credentialsProvider(awsCredentialsProvider).build();
      GetBucketLocationResponse locationResponse =
          s3ClientInterim.getBucketLocation(
              GetBucketLocationRequest.builder().bucket(bucketName).build());
      clientRegion = locationResponse.locationConstraintAsString();
      // In useast-1, the region is returned as null or empty which is equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
      if (clientRegion == null || clientRegion.isEmpty() || clientRegion.equals("US")) {
        clientRegion = "us-east-1";
      }
      s3ClientInterim.close();
    } else {
      clientRegion = region;
    }
    String serviceEndpoint = String.format("https://s3.%s.amazonaws.com", clientRegion);
    System.out.printf("S3 ServiceEndpoint: %s%n", serviceEndpoint);
    RetryPolicy retryPolicy = RetryPolicy.builder(RetryMode.STANDARD).numRetries(maxRetry).build();
    software.amazon.awssdk.core.client.config.ClientOverrideConfiguration overrideConfig =
        software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder()
            .retryPolicy(retryPolicy)
            .build();
    return S3Client.builder()
        .credentialsProvider(awsCredentialsProvider)
        .overrideConfiguration(overrideConfig)
        .region(Region.of(clientRegion))
        .endpointOverride(URI.create(serviceEndpoint))
        .build();
  }

  /**
   * Get an S3 client bundle usable for remote state operations.
   *
   * @param bucketName s3 bucket
   * @param region aws region, such as us-west-2, or null to detect
   * @param credsFile file containing aws credentials, or null for default
   * @param credsProfile profile to use from credentials file
   * @param maxRetry maximum number of retry attempts
   * @return S3ClientBundle containing sync and async clients
   */
  public static S3Util.S3ClientBundle createS3ClientBundle(
      String bucketName, String region, String credsFile, String credsProfile, int maxRetry) {
    S3Client s3Client = createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    S3AsyncClient s3AsyncClient =
        S3AsyncClient.crtBuilder()
            .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
            .region(s3Client.serviceClientConfiguration().region())
            .endpointOverride(s3Client.serviceClientConfiguration().endpointOverride().orElse(null))
            .build();
    return new S3Util.S3ClientBundle(s3Client, s3AsyncClient);
  }

  /**
   * Write UTF8 encoding of string to given file.
   *
   * @param content contents to write
   * @param file file to write to
   * @throws IOException
   */
  public static void writeStringToFile(String content, File file) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(StateUtils.toUTF8(content));
    }
  }

  /**
   * Write global state data to remote backend.
   *
   * @param s3Backend remote backend
   * @param serviceName service name
   * @param data global state utf8 data
   * @throws IOException on error writing global state
   */
  public static void writeGlobalStateDataToBackend(
      S3Backend s3Backend, String serviceName, byte[] data) throws IOException {
    s3Backend.uploadGlobalState(serviceName, data);
  }

  /**
   * Write index state data to remote backend.
   *
   * @param s3Backend remote backend
   * @param serviceName service name
   * @param indexResource index resource name
   * @param data index state utf8 data
   * @throws IOException on error writing index state
   */
  public static void writeIndexStateDataToBackend(
      S3Backend s3Backend, String serviceName, String indexResource, byte[] data)
      throws IOException {
    s3Backend.uploadIndexState(serviceName, indexResource, data);
  }

  /**
   * Validate state file data to ensure it is encoded properly and can be loaded into its state
   * message.
   *
   * @param configData utf8 encoded state data
   * @param isGlobalState if this is global or index state
   * @throws IOException
   */
  public static void validateConfigData(byte[] configData, boolean isGlobalState)
      throws IOException {
    String configStr = StateUtils.fromUTF8(configData);
    if (isGlobalState) {
      GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
      JsonFormat.parser().merge(configStr, builder);
      builder.build();
    } else {
      IndexStateInfo.Builder builder = IndexStateInfo.newBuilder();
      JsonFormat.parser().merge(configStr, builder);
      builder.build();
    }
  }

  /**
   * Get global state json string from remote backend.
   *
   * @param s3Backend remote backend
   * @param serviceName service name
   * @return global state json string
   * @throws IOException on error reading global state
   */
  public static String getGlobalStateFileContents(S3Backend s3Backend, String serviceName)
      throws IOException {
    if (!s3Backend.exists(serviceName, RemoteBackend.GlobalResourceType.GLOBAL_STATE)) {
      System.out.println("Global state does not exist for service: " + serviceName);
      return null;
    }
    InputStream stateStream = s3Backend.downloadGlobalState(serviceName);
    byte[] stateBytes = stateStream.readAllBytes();
    return StateUtils.fromUTF8(stateBytes);
  }

  /**
   * Get index state json string from remote backend.
   *
   * @param s3Backend remote backend
   * @param serviceName service name
   * @param indexResource index resource name
   * @return index state json string
   * @throws IOException on error reading index state
   */
  public static String getIndexStateFileContents(
      S3Backend s3Backend, String serviceName, String indexResource) throws IOException {
    if (!s3Backend.exists(
        serviceName, indexResource, RemoteBackend.IndexResourceType.INDEX_STATE)) {
      System.out.println(
          "Index state does not exist for service: " + serviceName + ", index: " + indexResource);
      return null;
    }
    InputStream stateStream = s3Backend.downloadIndexState(serviceName, indexResource);
    byte[] stateBytes = stateStream.readAllBytes();
    return StateUtils.fromUTF8(stateBytes);
  }

  /**
   * Get the resolved resource name for a given input resource. If the given resource is an index
   * name, the service global state is loaded to look up the index unique id. If the resource is for
   * global state, or exactResourceName is true, it is considered to already be resolved.
   *
   * @param s3Backend remote data backend
   * @param serviceName nrtsearch cluster service name
   * @param resource resource name to resolve
   * @param exactResourceName if the provided resource name is already resolved
   * @return resolved resource name
   * @throws IOException
   */
  public static String getResourceName(
      S3Backend s3Backend, String serviceName, String resource, boolean exactResourceName)
      throws IOException {
    if (isGlobalState(resource)) {
      System.out.println("Global state resource");
      return resource;
    }
    if (exactResourceName) {
      System.out.println("Index state resource: " + resource);
      return resource;
    }
    String globalStateContents = getGlobalStateFileContents(s3Backend, serviceName);
    if (globalStateContents == null) {
      throw new IllegalArgumentException(
          "Unable to load global state for cluster: \"" + serviceName + "\"");
    }
    GlobalStateInfo.Builder globalStateBuilder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(globalStateContents, globalStateBuilder);
    GlobalStateInfo globalState = globalStateBuilder.build();

    IndexGlobalState indexGlobalState = globalState.getIndicesMap().get(resource);
    if (indexGlobalState == null) {
      throw new IllegalArgumentException(
          "Unable to find index: \"" + resource + "\" in cluster: \"" + serviceName + "\"");
    }
    if (indexGlobalState.getId().isEmpty()) {
      throw new IllegalArgumentException("Index id is empty for index: \"" + resource + "\"");
    }
    String resolvedResource =
        BackendGlobalState.getUniqueIndexName(resource, indexGlobalState.getId());
    System.out.println("Index state resource: " + resolvedResource);
    return resolvedResource;
  }

  /**
   * Parse input string into index resource type.
   *
   * @param resourceType input string
   * @return index resource type
   * @throws IllegalArgumentException if invalid resource type name
   */
  public static RemoteBackend.IndexResourceType parseIndexResourceType(String resourceType) {
    return switch (resourceType) {
      case "INDEX_STATE" -> RemoteBackend.IndexResourceType.INDEX_STATE;
      case "POINT_STATE" -> RemoteBackend.IndexResourceType.POINT_STATE;
      case "WARMING_QUERIES" -> RemoteBackend.IndexResourceType.WARMING_QUERIES;
      default -> throw new IllegalArgumentException("Invalid index resource type: " + resourceType);
    };
  }
}
