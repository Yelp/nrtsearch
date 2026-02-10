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

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.toUTF8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

@CommandLine.Command(
    name = SnapshotCommand.SNAPSHOT,
    description = "Snapshot the current version of an index to a separate location in S3.")
public class SnapshotCommand implements Callable<Integer> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String SNAPSHOT = "snapshot";

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of nrtsearch cluster",
      required = true)
  private String serviceName;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of cluster index",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"--exactResourceName"},
      description = "If index resource name already has unique identifier")
  private boolean exactResourceName;

  @CommandLine.Option(
      names = {"-b", "--bucketName"},
      description = "Name of bucket containing state files",
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
      names = {"--snapshotRoot"},
      description = "Root s3 snapshot path, defaults to <serviceName>/snapshots/")
  private String snapshotRoot;

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
      s3Backend = new S3Backend(bucketName, false, S3Backend.DEFAULT_CONFIG, s3Client);
    }

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(s3Backend, serviceName, indexName, exactResourceName);

    String timeStringMs = TimeStringUtils.generateTimeStringMs();
    String resolvedSnapshotRoot = BackupCommandUtils.getSnapshotRoot(snapshotRoot, serviceName);
    String snapshotIndexDataRoot =
        BackupCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, resolvedIndexResource, timeStringMs);

    System.out.println(
        "Starting snapshot for index resource: "
            + resolvedIndexResource
            + ", snapshotIndexDataRoot: "
            + snapshotIndexDataRoot);

    long indexDataSizeBytes =
        copyIndexData(s3Backend, resolvedIndexResource, snapshotIndexDataRoot);
    copyIndexState(s3Backend, resolvedIndexResource, snapshotIndexDataRoot);
    copyWarmingQueries(s3Backend, resolvedIndexResource, snapshotIndexDataRoot);

    SnapshotMetadata metadata =
        new SnapshotMetadata(serviceName, resolvedIndexResource, timeStringMs, indexDataSizeBytes);
    writeMetadataFile(
        s3Client,
        bucketName,
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            resolvedSnapshotRoot, resolvedIndexResource, timeStringMs),
        metadata);

    System.out.println("Snapshot completed");
    return 0;
  }

  private long copyIndexData(
      S3Backend s3Backend, String resolvedIndexResource, String snapshotIndexDataRoot)
      throws IOException, InterruptedException {
    if (!s3Backend.exists(
        serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.POINT_STATE)) {
      throw new IllegalArgumentException("No data found for index: " + resolvedIndexResource);
    }
    byte[] pointStateBytes =
        s3Backend
            .downloadPointState(serviceName, resolvedIndexResource, null)
            .inputStream()
            .readAllBytes();
    NrtPointState nrtPointState = RemoteUtils.pointStateFromUtf8(pointStateBytes);
    Set<String> indexDataFiles =
        nrtPointState.files.entrySet().stream()
            .map(entry -> S3Backend.getIndexBackendFileName(entry.getKey(), entry.getValue()))
            .collect(Collectors.toSet());

    System.out.println("Version id: " + nrtPointState.version + ", files: " + indexDataFiles);

    String indexDataKeyPrefix = S3Backend.getIndexDataPrefix(serviceName, resolvedIndexResource);
    long totalDataSizeBytes = 0;

    S3TransferManager transferManager = s3Backend.getTransferManager();
    List<Copy> copyJobs = new ArrayList<>();
    for (String fileName : indexDataFiles) {
      String sourceKey = indexDataKeyPrefix + fileName;
      HeadObjectRequest headRequest =
          HeadObjectRequest.builder().bucket(bucketName).key(sourceKey).build();
      totalDataSizeBytes += s3Backend.getS3().headObject(headRequest).contentLength();
      CopyObjectRequest copyObjectRequest =
          CopyObjectRequest.builder()
              .sourceBucket(bucketName)
              .sourceKey(sourceKey)
              .destinationBucket(bucketName)
              .destinationKey(snapshotIndexDataRoot + fileName)
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

    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(snapshotIndexDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE)
            .contentLength((long) pointStateBytes.length)
            .build();
    s3Backend.getS3().putObject(putObjectRequest, RequestBody.fromBytes(pointStateBytes));
    return totalDataSizeBytes;
  }

  private void copyIndexState(
      S3Backend s3Backend, String resolvedIndexResource, String snapshotIndexDataRoot)
      throws IOException {
    if (!s3Backend.exists(
        serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.INDEX_STATE)) {
      throw new IllegalArgumentException("No state found for index: " + resolvedIndexResource);
    }
    String prefix =
        S3Backend.getIndexResourcePrefix(
            serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.INDEX_STATE);
    String currentStateVersion = s3Backend.getCurrentResourceName(prefix);
    System.out.println("Current index state version: " + currentStateVersion);

    CopyObjectRequest copyRequest =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(prefix + currentStateVersion)
            .destinationBucket(bucketName)
            .destinationKey(snapshotIndexDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE)
            .build();
    s3Backend.getS3().copyObject(copyRequest);
  }

  private void copyWarmingQueries(
      S3Backend s3Backend, String resolvedIndexResource, String snapshotIndexDataRoot)
      throws IOException {
    if (s3Backend.exists(
        serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.WARMING_QUERIES)) {
      String resourcePrefix =
          S3Backend.getIndexResourcePrefix(
              serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.WARMING_QUERIES);
      String currentWarmingQueriesVersion = s3Backend.getCurrentResourceName(resourcePrefix);
      System.out.println("Current warming queries version: " + currentWarmingQueriesVersion);

      CopyObjectRequest copyRequest =
          CopyObjectRequest.builder()
              .sourceBucket(bucketName)
              .sourceKey(resourcePrefix + currentWarmingQueriesVersion)
              .destinationBucket(bucketName)
              .destinationKey(snapshotIndexDataRoot + BackupCommandUtils.SNAPSHOT_WARMING_QUERIES)
              .build();
      s3Backend.getS3().copyObject(copyRequest);
    } else {
      System.out.println("No warming queries present for index, skipping copy");
    }
  }

  private void writeMetadataFile(
      S3Client s3Client, String bucketName, String metadataFileKey, SnapshotMetadata metadata)
      throws IOException {
    String metadataFileStr = OBJECT_MAPPER.writeValueAsString(metadata);
    System.out.println(
        "Writing metadata file key: " + metadataFileKey + ", content: " + metadataFileStr);
    byte[] fileData = toUTF8(metadataFileStr);
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(metadataFileKey)
            .contentLength((long) fileData.length)
            .build();
    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(fileData));
  }
}
