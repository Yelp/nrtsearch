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
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.fromUTF8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

@CommandLine.Command(
    name = RestoreCommand.RESTORE,
    description = "Restore snapshot of index data into existing cluster.")
public class RestoreCommand implements Callable<Integer> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String RESTORE = "restore";

  @CommandLine.Option(
      names = {"--restoreServiceName"},
      description = "Name of nrtsearch cluster to restore data to",
      required = true)
  private String restoreServiceName;

  @CommandLine.Option(
      names = {"--restoreIndexName"},
      description = "Name of restored index",
      required = true)
  private String restoreIndexName;

  @CommandLine.Option(
      names = {"--restoreIndexId"},
      description =
          "Time string (UTC) to use for restored index, must be of the form yyyyMMddHHmmssSSS. Defaults to current time.")
  private String restoreIndexId;

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "Name of bucket containing index files",
      required = true)
  private String bucketName;

  @CommandLine.Option(
      names = {"--region"},
      description = "AWS region name, such as us-west-1, us-west-2, us-east-1")
  private String region;

  @CommandLine.Option(
      names = {"-c", "--credsFile"},
      description =
          "File holding AWS credentials; Will use DefaultCredentialProvider if this is unset.")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file; Neglected when credsFile is unset.",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"--snapshotIndexIdentifier"},
      description = "Index identifier for snapshot, in the form <index_name>-<time_string>",
      required = true)
  private String snapshotIndexIdentifier;

  @CommandLine.Option(
      names = {"--snapshotTimeString"},
      description =
          "Time string of snapshot data to restore, must be of the form yyyyMMddHHmmssSSS",
      required = true)
  private String snapshotTimeString;

  @CommandLine.Option(
      names = {"--snapshotRoot"},
      description =
          "Root s3 snapshot path, defaults to <snapshotServiceName>/snapshots/ either this or snapshotServiceName must be specified")
  private String snapshotRoot;

  @CommandLine.Option(
      names = {"--snapshotServiceName"},
      description =
          "Name of nrtsearch cluster for snapshot, either this or snapshotRoot must be specified")
  private String snapshotServiceName;

  @CommandLine.Option(
      names = {"--copyThreads"},
      description =
          "Number of threads to use when copying index files, (default: ${DEFAULT-VALUE})",
      defaultValue = "10")
  int copyThreads;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  private S3Client s3Client;
  private software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient;
  private S3TransferManager transferManager;

  @VisibleForTesting
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @VisibleForTesting
  void setS3AsyncClient(software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
  }

  @VisibleForTesting
  void setTransferManager(S3TransferManager transferManager) {
    this.transferManager = transferManager;
  }

  @Override
  public Integer call() throws Exception {
    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    S3Backend s3Backend;
    if (s3AsyncClient != null && transferManager != null) {
      s3Backend =
          new S3Backend(
              bucketName,
              false,
              S3Backend.DEFAULT_CONFIG,
              s3Client,
              s3AsyncClient,
              transferManager);
    } else {
      s3Backend =
          new S3Backend(
              bucketName,
              false,
              S3Backend.DEFAULT_CONFIG,
              new S3Util.S3ClientBundle(s3Client, null));
    }

    String resolvedSnapshotRoot =
        BackupCommandUtils.getSnapshotRoot(snapshotRoot, snapshotServiceName);
    String snapshotDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    String restoreIndexResource = getRestoreIndexResource();

    System.out.println(
        "String restore of index, snapshotDataRoot: "
            + snapshotDataRoot
            + ", restoreService: "
            + restoreServiceName
            + ", restoreIndex: "
            + restoreIndexResource);

    checkRestoreIndexNotExists(s3Client, restoreIndexResource);

    SnapshotMetadata metadata = loadMetadata(s3Client, resolvedSnapshotRoot);
    restoreIndexData(s3Backend, restoreIndexResource, snapshotDataRoot);
    restoreIndexState(s3Backend, restoreIndexResource, snapshotDataRoot);
    maybeRestoreWarmingQueries(s3Backend, restoreIndexResource, snapshotDataRoot);

    System.out.println("Restore completed");
    return 0;
  }

  private void restoreIndexData(
      S3Backend s3Backend, String restoreIndexResource, String snapshotDataRoot)
      throws IOException, InterruptedException {
    GetObjectRequest getRequest =
        GetObjectRequest.builder()
            .bucket(bucketName)
            .key(snapshotDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE)
            .build();
    byte[] pointStateBytes = IOUtils.toByteArray(s3Client.getObject(getRequest));
    NrtPointState nrtPointState = RemoteUtils.pointStateFromUtf8(pointStateBytes);
    String pointStateFileName = S3Backend.getPointStateFileName(nrtPointState);
    String restorePointStatePrefix =
        S3Backend.getIndexResourcePrefix(
            restoreServiceName, restoreIndexResource, RemoteBackend.IndexResourceType.POINT_STATE);

    String restoreDataPrefix =
        S3Backend.getIndexDataPrefix(restoreServiceName, restoreIndexResource);
    Set<String> backendIndexFiles =
        nrtPointState.files.entrySet().stream()
            .map(entry -> S3Backend.getIndexBackendFileName(entry.getKey(), entry.getValue()))
            .collect(Collectors.toSet());

    System.out.println(
        "Restoring point state to key: " + restorePointStatePrefix + pointStateFileName);
    PutObjectRequest putRequest =
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(restorePointStatePrefix + pointStateFileName)
            .contentLength((long) pointStateBytes.length)
            .build();
    s3Client.putObject(putRequest, RequestBody.fromBytes(pointStateBytes));

    System.out.println("Restoring index data: " + backendIndexFiles);

    S3TransferManager transferManager = s3Backend.getTransferManager();
    List<Copy> copyJobs = new ArrayList<>();
    for (String fileName : backendIndexFiles) {
      CopyObjectRequest copyObjectRequest =
          CopyObjectRequest.builder()
              .sourceBucket(bucketName)
              .sourceKey(snapshotDataRoot + fileName)
              .destinationBucket(bucketName)
              .destinationKey(restoreDataPrefix + fileName)
              .build();
      final String finalFileName = fileName;
      Copy copy =
          transferManager.copy(CopyRequest.builder().copyObjectRequest(copyObjectRequest).build());
      copyJobs.add(copy);
      System.out.println("Started copy: " + finalFileName);
    }
    for (Copy copyJob : copyJobs) {
      CompletedCopy completedCopy = copyJob.completionFuture().join();
      System.out.println("Completed copy");
    }
    s3Backend.setCurrentResource(restorePointStatePrefix, pointStateFileName);
  }

  private void restoreIndexState(
      S3Backend s3Backend, String restoreIndexResource, String snapshotDataRoot)
      throws IOException {
    String restoreIndexStatePrefix =
        S3Backend.getIndexResourcePrefix(
            restoreServiceName, restoreIndexResource, RemoteBackend.IndexResourceType.INDEX_STATE);
    String stateFileName = S3Backend.getIndexStateFileName();

    System.out.println("Restoring index state to key: " + restoreIndexStatePrefix + stateFileName);
    CopyObjectRequest copyRequest =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(snapshotDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE)
            .destinationBucket(bucketName)
            .destinationKey(restoreIndexStatePrefix + stateFileName)
            .build();
    s3Client.copyObject(copyRequest);
    s3Backend.setCurrentResource(restoreIndexStatePrefix, stateFileName);
  }

  private void maybeRestoreWarmingQueries(
      S3Backend s3Backend, String restoreIndexResource, String snapshotDataRoot) {
    String snapshotWarmingQueriesKey =
        snapshotDataRoot + BackupCommandUtils.SNAPSHOT_WARMING_QUERIES;
    try {
      s3Client.headObject(
          HeadObjectRequest.builder().bucket(bucketName).key(snapshotWarmingQueriesKey).build());
    } catch (NoSuchKeyException e) {
      System.out.println("Warming queries not present in snapshot, skipping");
      return;
    }
    String restoreWarmingQueriesPrefix =
        S3Backend.getIndexResourcePrefix(
            restoreServiceName,
            restoreIndexResource,
            RemoteBackend.IndexResourceType.WARMING_QUERIES);
    String warmingQueriesFilename = S3Backend.getWarmingQueriesFileName();

    System.out.println(
        "Restoring warming queries to key: "
            + restoreWarmingQueriesPrefix
            + warmingQueriesFilename);
    CopyObjectRequest copyRequest =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(snapshotWarmingQueriesKey)
            .destinationBucket(bucketName)
            .destinationKey(restoreWarmingQueriesPrefix + warmingQueriesFilename)
            .build();
    s3Client.copyObject(copyRequest);
    s3Backend.setCurrentResource(restoreWarmingQueriesPrefix, warmingQueriesFilename);
  }

  private SnapshotMetadata loadMetadata(S3Client s3Client, String snapshotRoot) throws IOException {
    String metadataFileKey =
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            snapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    try {
      s3Client.headObject(
          HeadObjectRequest.builder().bucket(bucketName).key(metadataFileKey).build());
    } catch (NoSuchKeyException e) {
      throw new IllegalArgumentException("Metadata file does not exist: " + metadataFileKey);
    }
    System.out.println("Loading metadata key: " + metadataFileKey);
    GetObjectRequest getRequest =
        GetObjectRequest.builder().bucket(bucketName).key(metadataFileKey).build();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(s3Client.getObject(getRequest), byteArrayOutputStream);
    String fileContent = fromUTF8(byteArrayOutputStream.toByteArray());
    System.out.println("Contents: " + fileContent);
    return OBJECT_MAPPER.readValue(fileContent, SnapshotMetadata.class);
  }

  private void checkRestoreIndexNotExists(S3Client s3Client, String restoreIndexResource) {
    String restorePrefix = restoreServiceName + "/" + restoreIndexResource;
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(restorePrefix).build();
    ListObjectsV2Response result = s3Client.listObjectsV2(listRequest);
    List<S3Object> objects = result.contents();
    if (!objects.isEmpty()) {
      throw new IllegalArgumentException(
          "Data exists at restore location, found: " + objects.get(0).key());
    }
  }

  private String getRestoreIndexResource() {
    return BackendGlobalState.getUniqueIndexName(restoreIndexName, getRestoreIndexId());
  }

  private String getRestoreIndexId() {
    if (restoreIndexId == null) {
      return TimeStringUtils.generateTimeStringMs();
    } else {
      if (!TimeStringUtils.isTimeStringMs(restoreIndexId)) {
        throw new IllegalStateException(
            "restoreIndexId must be a time string of the form yyyyMMddHHmmssSSS, got: "
                + restoreIndexId);
      }
      return restoreIndexId;
    }
  }
}
