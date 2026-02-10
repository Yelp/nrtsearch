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
package com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.toUTF8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

@CommandLine.Command(
    name = SnapshotIncrementalCommand.SNAPSHOT_INCREMENTAL,
    description =
        "Snapshot the current version of an index to a separate location in S3. Legacy command for use with v0 cluster data.")
public class SnapshotIncrementalCommand implements Callable<Integer> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String SNAPSHOT_INCREMENTAL = "snapshotIncremental";

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
          LegacyStateCommandUtils.createS3Client(
              bucketName, region, credsFile, credsProfile, maxRetry);
    }
    LegacyVersionManager versionManager = new LegacyVersionManager(s3Client, bucketName);

    String resolvedIndexResource =
        LegacyStateCommandUtils.getResourceName(
            versionManager, serviceName, indexName, exactResourceName);

    long currentTimestampMs = System.currentTimeMillis();
    String resolvedSnapshotRoot =
        IncrementalCommandUtils.getSnapshotRoot(snapshotRoot, serviceName);
    String snapshotIndexDataRoot =
        IncrementalCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, resolvedIndexResource, currentTimestampMs);

    System.out.println(
        "Starting snapshot for index resource: "
            + resolvedIndexResource
            + ", snapshotIndexDataRoot: "
            + snapshotIndexDataRoot);

    long indexDataSizeBytes =
        copyIndexData(versionManager, resolvedIndexResource, snapshotIndexDataRoot);
    copyIndexState(versionManager, resolvedIndexResource, snapshotIndexDataRoot);
    copyWarmingQueries(versionManager, resolvedIndexResource, snapshotIndexDataRoot);

    SnapshotMetadata metadata =
        new SnapshotMetadata(
            serviceName, resolvedIndexResource, currentTimestampMs, indexDataSizeBytes);
    writeMetadataFile(
        s3Client,
        bucketName,
        IncrementalCommandUtils.getSnapshotIndexMetadataKey(
            resolvedSnapshotRoot, resolvedIndexResource, currentTimestampMs),
        metadata);

    System.out.println("Snapshot completed");
    return 0;
  }

  private long copyIndexData(
      LegacyVersionManager versionManager,
      String resolvedIndexResource,
      String snapshotIndexDataRoot)
      throws IOException, InterruptedException {
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(resolvedIndexResource);
    long currentDataVersion = versionManager.getLatestVersionNumber(serviceName, indexDataResource);
    System.out.println("Current index data version: " + currentDataVersion);
    if (currentDataVersion < 0) {
      throw new IllegalArgumentException("No data found for index: " + indexDataResource);
    }
    String dataVersionId =
        versionManager.getVersionString(
            serviceName, indexDataResource, String.valueOf(currentDataVersion));
    Set<String> indexDataFiles =
        IncrementalCommandUtils.getVersionFiles(
            versionManager.getS3(), bucketName, serviceName, indexDataResource, dataVersionId);

    System.out.println("Version id: " + dataVersionId + ", files: " + indexDataFiles);

    String indexDataKeyPrefix =
        IncrementalCommandUtils.getDataKeyPrefix(serviceName, indexDataResource);
    long totalDataSizeBytes = 0;

    S3TransferManager transferManagerToUse = this.transferManager;
    boolean shouldCloseTransferManager = false;
    if (transferManagerToUse == null) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(copyThreads);
      software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient =
          software.amazon.awssdk.services.s3.S3AsyncClient.crtBuilder()
              .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
              .region(s3Client.serviceClientConfiguration().region())
              .build();
      transferManagerToUse = S3TransferManager.builder().s3Client(s3AsyncClient).build();
      shouldCloseTransferManager = true;
    }
    try {
      List<Copy> copyJobs = new ArrayList<>();
      for (String fileName : indexDataFiles) {
        String sourceKey = indexDataKeyPrefix + fileName;
        HeadObjectRequest headRequest =
            HeadObjectRequest.builder().bucket(bucketName).key(sourceKey).build();
        HeadObjectResponse headResponse = versionManager.getS3().headObject(headRequest);
        totalDataSizeBytes += headResponse.contentLength();
        CopyObjectRequest copyObjectRequest =
            CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(sourceKey)
                .destinationBucket(bucketName)
                .destinationKey(snapshotIndexDataRoot + fileName)
                .build();
        final String finalFileName = fileName;
        Copy copy =
            transferManagerToUse.copy(
                CopyRequest.builder().copyObjectRequest(copyObjectRequest).build());
        copyJobs.add(copy);
        System.out.println("Started copy: " + finalFileName);
      }
      for (Copy copyJob : copyJobs) {
        CompletedCopy completedCopy = copyJob.completionFuture().join();
        System.out.println("Completed copy");
      }
    } finally {
      if (shouldCloseTransferManager) {
        transferManagerToUse.close();
      }
    }
    CopyObjectRequest copyRequest1 =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(
                IncrementalCommandUtils.getDataKeyPrefix(serviceName, indexDataResource)
                    + dataVersionId)
            .destinationBucket(bucketName)
            .destinationKey(snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_FILES)
            .build();
    versionManager.getS3().copyObject(copyRequest1);
    return totalDataSizeBytes;
  }

  private void copyIndexState(
      LegacyVersionManager versionManager,
      String resolvedIndexResource,
      String snapshotIndexDataRoot)
      throws IOException {
    String indexStateResource =
        LegacyStateCommandUtils.getIndexStateResource(resolvedIndexResource);
    long currentStateVersion =
        versionManager.getLatestVersionNumber(serviceName, indexStateResource);
    System.out.println("Current index state version: " + currentStateVersion);
    if (currentStateVersion < 0) {
      throw new IllegalArgumentException("No state found for index: " + indexStateResource);
    }
    String stateVersionId =
        versionManager.getVersionString(
            serviceName, indexStateResource, String.valueOf(currentStateVersion));

    System.out.println("Version id: " + stateVersionId);

    versionManager
        .getS3()
        .copyObject(
            CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(
                    LegacyStateCommandUtils.getStateKey(
                        serviceName, indexStateResource, stateVersionId))
                .destinationBucket(bucketName)
                .destinationKey(
                    snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_STATE_FILE)
                .build());
  }

  private void copyWarmingQueries(
      LegacyVersionManager versionManager,
      String resolvedIndexResource,
      String snapshotIndexDataRoot)
      throws IOException {
    String indexWarmingQueriesResource =
        IncrementalCommandUtils.getWarmingQueriesResource(resolvedIndexResource);
    long currentWarmingQueriesVersion =
        versionManager.getLatestVersionNumber(serviceName, indexWarmingQueriesResource);
    System.out.println("Current warming queries version: " + currentWarmingQueriesVersion);
    if (currentWarmingQueriesVersion >= 0) {
      String warmingQueriesVersionId =
          versionManager.getVersionString(
              serviceName,
              indexWarmingQueriesResource,
              String.valueOf(currentWarmingQueriesVersion));

      System.out.println("Version id: " + warmingQueriesVersionId);

      versionManager
          .getS3()
          .copyObject(
              CopyObjectRequest.builder()
                  .sourceBucket(bucketName)
                  .sourceKey(
                      IncrementalCommandUtils.getWarmingQueriesKeyPrefix(
                              serviceName, indexWarmingQueriesResource)
                          + warmingQueriesVersionId)
                  .destinationBucket(bucketName)
                  .destinationKey(
                      snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_WARMING_QUERIES)
                  .build());
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
    PutObjectRequest request =
        PutObjectRequest.builder()
            .bucket(bucketName)
            .key(metadataFileKey)
            .contentLength((long) fileData.length)
            .build();
    s3Client.putObject(request, RequestBody.fromBytes(fileData));
  }
}
