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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

public class StateCommandUtils {
  private StateCommandUtils() {}

  /**
   * Get an S3 client usable for remote state operations.
   *
   * @param bucketName s3 bucket
   * @param region aws region, such as us-west-2, or null to detect
   * @param credsFile file containing aws credentials, or null for default
   * @param credsProfile profile to use from credentials file
   * @return s3 client
   */
  public static AmazonS3 createS3Client(
      String bucketName, String region, String credsFile, String credsProfile, int maxRetry) {
    ProfilesConfigFile profilesConfigFile = null;
    if (credsFile != null) {
      Path botoCfgPath = Paths.get(credsFile);
      profilesConfigFile = new ProfilesConfigFile(botoCfgPath.toFile());
    }
    AWSCredentialsProvider awsCredentialsProvider =
        new ProfileCredentialsProvider(profilesConfigFile, credsProfile);

    String clientRegion;
    if (region == null) {
      AmazonS3 s3ClientInterim =
          AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
      clientRegion = s3ClientInterim.getBucketLocation(bucketName);
      // In useast-1, the region is returned as "US" which is an equivalent to "us-east-1"
      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html#US_Standard
      // However, this causes an UnknownHostException so we override it to the full region name
      if (clientRegion.equals("US")) {
        clientRegion = "us-east-1";
      }
    } else {
      clientRegion = region;
    }
    String serviceEndpoint = String.format("s3.%s.amazonaws.com", clientRegion);
    System.out.printf("S3 ServiceEndpoint: %s%n", serviceEndpoint);
    RetryPolicy retryPolicy =
        new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
            PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
            maxRetry,
            true);
    ClientConfiguration clientConfiguration =
        new ClientConfiguration().withRetryPolicy(retryPolicy);
    return AmazonS3ClientBuilder.standard()
        .withCredentials(awsCredentialsProvider)
        .withClientConfiguration(clientConfiguration)
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, clientRegion))
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
      VersionManager versionManager, String serviceName, String resourceName, String stateFileName)
      throws IOException {
    String backendResourceName =
        IndexBackupUtils.isBackendGlobalState(resourceName)
            ? resourceName
            : getIndexStateResource(resourceName);
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
    if (!versionManager
        .getS3()
        .doesObjectExist(versionManager.getBucketName(), absoluteResourcePath)) {
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
      AmazonS3 s3Client, String bucketName, String key, String stateFileName) throws IOException {
    S3Object stateObject = s3Client.getObject(bucketName, key);
    InputStream contents = stateObject.getObjectContent();
    String stateStr = null;
    try (TarArchiveInputStream tarArchiveInputStream =
        new TarArchiveInputStream(new LZ4FrameInputStream(contents))) {
      for (TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
          tarArchiveEntry != null;
          tarArchiveEntry = tarArchiveInputStream.getNextTarEntry()) {
        if (tarArchiveEntry.getName().endsWith(stateFileName)) {
          byte[] fileData = tarArchiveInputStream.readNBytes((int) tarArchiveEntry.getSize());
          stateStr = StateUtils.fromUTF8(fileData);
        }
      }
    }
    return stateStr;
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
   * Write the given state file data to the remote backend, and bless it.
   *
   * @param versionManager state version manager
   * @param serviceName nrtsearch cluster service name
   * @param resourceName state resource name, for index this must include unique identifier
   * @param stateFileName name of state file in archive
   * @param data utf8 encoded state file data
   * @throws IOException
   */
  public static void writeStateDataToBackend(
      VersionManager versionManager,
      String serviceName,
      String resourceName,
      String stateFileName,
      byte[] data)
      throws IOException {
    byte[] archiveData = buildStateFileArchive(resourceName, stateFileName, data);

    String versionStr = UUID.randomUUID().toString();
    String backendResourceName =
        IndexBackupUtils.isBackendGlobalState(resourceName)
            ? resourceName
            : resourceName + IndexBackupUtils.INDEX_STATE_SUFFIX;
    String absoluteResourcePath = getStateKey(serviceName, backendResourceName, versionStr);
    System.out.println("Writing to resource: " + absoluteResourcePath);
    writeDataToS3(
        versionManager.getS3(), versionManager.getBucketName(), absoluteResourcePath, archiveData);
    versionManager.blessVersion(serviceName, backendResourceName, versionStr);
  }

  /**
   * Write a data buffer to an S3 object.
   *
   * @param s3Client s3 client
   * @param bucketName bucket name
   * @param key key for object to write
   * @param data data buffer
   */
  public static void writeDataToS3(AmazonS3 s3Client, String bucketName, String key, byte[] data) {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(data.length);
    s3Client.putObject(
        new PutObjectRequest(bucketName, key, new ByteArrayInputStream(data), metadata));
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
      VersionManager versionManager, String serviceName, String resource, boolean exactResourceName)
      throws IOException {
    if (IndexBackupUtils.isBackendGlobalState(resource)) {
      System.out.println("Global state resource");
      return resource;
    }
    if (exactResourceName) {
      System.out.println("Index state resource: " + resource);
      return resource;
    }
    String globalStateContents =
        getStateFileContents(
            versionManager,
            serviceName,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
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
   * Given an index resource (index-UUID), produce the index state resource.
   *
   * @param indexResource index resource
   * @return index state resource
   */
  public static String getIndexStateResource(String indexResource) {
    return indexResource + IndexBackupUtils.INDEX_STATE_SUFFIX;
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
   * Get the S3 key prefix (with trailing slash) for all index state objects for a given index.
   *
   * @param serviceName nrtsearch cluster service name
   * @param indexStateResource index state resource
   * @return S3 index state key prefix
   */
  public static String getStateKeyPrefix(String serviceName, String indexStateResource) {
    return String.format("%s/%s/", serviceName, indexStateResource);
  }
}
