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
package com.yelp.nrtsearch.tools.nrt_utils.legacy.state;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.fromUTF8;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class LegacyStateCommandUtils {
  private static final String INDEX_STATE_SUFFIX = "-state";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";
  private static final String GLOBAL_STATE_FILE = "state.json";

  private LegacyStateCommandUtils() {}

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
      Path botoCfgPath = Paths.get(credsFile);
      ProfileFile profileFile = ProfileFile.builder().content(botoCfgPath).build();
      awsCredentialsProvider =
          ProfileCredentialsProvider.builder()
              .profileFile(profileFile)
              .profileName(credsProfile)
              .build();
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.create();
    }

    String clientRegion;
    if (region == null) {
      S3Client s3ClientInterim =
          S3Client.builder().credentialsProvider(awsCredentialsProvider).build();
      String bucketLocation =
          s3ClientInterim
              .getBucketLocation(GetBucketLocationRequest.builder().bucket(bucketName).build())
              .locationConstraintAsString();
      // In useast-1, the region is returned as empty string or "us-east-1"
      // Handle the empty string case
      if (bucketLocation == null || bucketLocation.isEmpty()) {
        clientRegion = "us-east-1";
      } else {
        clientRegion = bucketLocation;
      }
      s3ClientInterim.close();
    } else {
      clientRegion = region;
    }
    String serviceEndpoint = String.format("https://s3.%s.amazonaws.com", clientRegion);
    System.out.printf("S3 ServiceEndpoint: %s%n", serviceEndpoint);
    RetryPolicy retryPolicy =
        RetryPolicy.builder()
            .numRetries(maxRetry)
            .retryCondition(RetryCondition.defaultRetryCondition())
            .backoffStrategy(BackoffStrategy.defaultStrategy())
            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
            .build();
    return S3Client.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(Region.of(clientRegion))
        .endpointOverride(URI.create(serviceEndpoint))
        .overrideConfiguration(c -> c.retryPolicy(retryPolicy))
        .build();
  }

  /**
   * Get the contents of the state file stored in remote state.
   *
   * @param versionManager state version manager
   * @param serviceName nrtsearch cluster service name
   * @param resourceName state resource name, for index this must include unique identifier
   * @param stateFileName name of state file in archive
   * @return state file contents as string
   * @throws IOException
   */
  public static String getStateFileContents(
      LegacyVersionManager versionManager,
      String serviceName,
      String resourceName,
      String stateFileName)
      throws IOException {
    String backendResourceName =
        isBackendGlobalState(resourceName) ? resourceName : getIndexStateResource(resourceName);
    long currentVersion = versionManager.getLatestVersionNumber(serviceName, backendResourceName);
    if (currentVersion == -1) {
      System.out.println(
          "No state exists for service: " + serviceName + ", resource: " + backendResourceName);
      return null;
    }
    System.out.println("Current version: " + currentVersion);
    String versionStr =
        versionManager.getVersionString(
            serviceName, backendResourceName, String.valueOf(currentVersion));
    System.out.println("Version string: " + versionStr);

    String absoluteResourcePath = getStateKey(serviceName, backendResourceName, versionStr);
    try {
      versionManager
          .getS3()
          .headObject(
              HeadObjectRequest.builder()
                  .bucket(versionManager.getBucketName())
                  .key(absoluteResourcePath)
                  .build());
    } catch (NoSuchKeyException e) {
      System.out.println("Resource does not exist: " + absoluteResourcePath);
      return null;
    }

    String stateStr =
        getStateFileContentsFromS3(
            versionManager.getS3(),
            versionManager.getBucketName(),
            absoluteResourcePath,
            stateFileName);
    if (stateStr == null) {
      System.out.println("No state file found in archive");
    }
    return stateStr;
  }

  /**
   * Get the contents of a state file archive stored in S3.
   *
   * @param s3Client s3 client
   * @param bucketName bucket name
   * @param key key for state file archive
   * @param stateFileName name of state file within archive
   * @return contents of state file as string
   * @throws IOException
   */
  public static String getStateFileContentsFromS3(
      S3Client s3Client, String bucketName, String key, String stateFileName) throws IOException {
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(bucketName).key(key).build();
    ResponseInputStream<GetObjectResponse> responseInputStream =
        s3Client.getObject(getObjectRequest);
    String stateStr = null;
    try (TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(new LZ4FrameInputStream(responseInputStream))) {
      for (TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextEntry();
          tarArchiveEntry != null;
          tarArchiveEntry = tarArchiveInputStream.getNextEntry()) {
        if (tarArchiveEntry.getName().endsWith(stateFileName)) {
          byte[] fileData = tarArchiveInputStream.readNBytes((int) tarArchiveEntry.getSize());
          stateStr = fromUTF8(fileData);
        }
      }
    }
    return stateStr;
  }

  /**
   * Given the data of a state file, build an archive of the expected format and return its data.
   *
   * @param resourceName name of index resource
   * @param stateFileName name of state file within archive
   * @param data UTF-8 representation of state file
   * @return buffer containing archive data
   * @throws IOException
   */
  public static byte[] buildStateFileArchive(String resourceName, String stateFileName, byte[] data)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (ArchiveOutputStream archiveOutputStream =
        new TarArchiveOutputStream(new LZ4FrameOutputStream(outputStream))) {
      TarArchiveEntry entry = new TarArchiveEntry(resourceName + "/");
      archiveOutputStream.putArchiveEntry(entry);
      archiveOutputStream.closeArchiveEntry();

      entry = new TarArchiveEntry(resourceName + "/" + stateFileName);
      entry.setSize(data.length);
      archiveOutputStream.putArchiveEntry(entry);
      try (InputStream dataInputStream = new ByteArrayInputStream(data)) {
        IOUtils.copy(dataInputStream, archiveOutputStream);
      }
      archiveOutputStream.closeArchiveEntry();
    }
    return outputStream.toByteArray();
  }

  /**
   * Get the resolved resource name for a given input resource. If the given resource is an index
   * name, the service global state is loaded to look up the index unique id. If the resource is for
   * global state, or exactResourceName is true, it is considered to already be resolved.
   *
   * @param versionManager state version manager
   * @param serviceName nrtsearch cluster service name
   * @param resource resource name to resolve
   * @param exactResourceName if the provided resource name is already resolved
   * @return resolved resource name
   * @throws IOException
   */
  public static String getResourceName(
      LegacyVersionManager versionManager,
      String serviceName,
      String resource,
      boolean exactResourceName)
      throws IOException {
    if (isBackendGlobalState(resource)) {
      System.out.println("Global state resource");
      return resource;
    }
    if (exactResourceName) {
      System.out.println("Index state resource: " + resource);
      return resource;
    }
    String globalStateContents =
        getStateFileContents(versionManager, serviceName, GLOBAL_STATE_RESOURCE, GLOBAL_STATE_FILE);
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
    String resolvedResource = getUniqueIndexName(resource, indexGlobalState.getId());
    System.out.println("Index state resource: " + resolvedResource);
    return resolvedResource;
  }

  /**
   * Given an index resource (index-UUID), produce the index state resource.
   *
   * @param indexResource index resource
   * @return index state resource
   */
  public static String getIndexStateResource(String indexResource) {
    return indexResource + INDEX_STATE_SUFFIX;
  }

  /**
   * Get the S3 key for an index state object.
   *
   * @param serviceName nrtsearch cluster service name
   * @param indexStateResource index state resource
   * @param versionStr version UUID
   * @return S3 key for state object
   */
  public static String getStateKey(
      String serviceName, String indexStateResource, String versionStr) {
    return String.format("%s/%s/%s", serviceName, indexStateResource, versionStr);
  }

  /**
   * Build unique index name from index name and instance id (UUID).
   *
   * @param indexName index name
   * @param id instance id
   * @return unique index identifier
   * @throws NullPointerException if either parameter is null
   */
  public static String getUniqueIndexName(String indexName, String id) {
    Objects.requireNonNull(indexName);
    Objects.requireNonNull(id);
    return indexName + "-" + id;
  }

  public static boolean isBackendGlobalState(String resourceName) {
    return GLOBAL_STATE_RESOURCE.equals(resourceName);
  }
}
