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
package com.yelp.nrtsearch.tools.nrt_utils.incremental;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import picocli.CommandLine;

@CommandLine.Command(
    name = SnapshotIncrementalCommand.SNAPSHOT_INCREMENTAL,
    description = "Snapshot the current version of an index to a separate location in S3")
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
      description = "File holding AWS credentials, uses default locations if not set")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file",
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

  private AmazonS3 s3Client;

  @VisibleForTesting
  void setS3Client(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public Integer call() throws Exception {
    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    VersionManager versionManager = new VersionManager(s3Client, bucketName);

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(
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
      VersionManager versionManager, String resolvedIndexResource, String snapshotIndexDataRoot)
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

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(copyThreads);
    TransferManager transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3Client)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(true)
            .build();
    try {
      List<Copy> copyJobs = new ArrayList<>();
      for (String fileName : indexDataFiles) {
        String sourceKey = indexDataKeyPrefix + fileName;
        totalDataSizeBytes +=
            versionManager.getS3().getObjectMetadata(bucketName, sourceKey).getContentLength();
        CopyObjectRequest copyObjectRequest =
            new CopyObjectRequest(
                bucketName, sourceKey, bucketName, snapshotIndexDataRoot + fileName);
        final String finalFileName = fileName;
        Copy copy =
            transferManager.copy(
                copyObjectRequest,
                (transfer, state) ->
                    System.out.println("Transfer: " + finalFileName + ", state: " + state));
        copyJobs.add(copy);
      }
      for (Copy copyJob : copyJobs) {
        copyJob.waitForCopyResult();
      }
    } finally {
      transferManager.shutdownNow(false);
    }
    versionManager
        .getS3()
        .copyObject(
            bucketName,
            IncrementalCommandUtils.getDataKeyPrefix(serviceName, indexDataResource)
                + dataVersionId,
            bucketName,
            snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_FILES);
    return totalDataSizeBytes;
  }

  private void copyIndexState(
      VersionManager versionManager, String resolvedIndexResource, String snapshotIndexDataRoot)
      throws IOException {
    String indexStateResource = StateCommandUtils.getIndexStateResource(resolvedIndexResource);
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
            bucketName,
            StateCommandUtils.getStateKey(serviceName, indexStateResource, stateVersionId),
            bucketName,
            snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_STATE_FILE);
  }

  private void copyWarmingQueries(
      VersionManager versionManager, String resolvedIndexResource, String snapshotIndexDataRoot)
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
              bucketName,
              IncrementalCommandUtils.getWarmingQueriesKeyPrefix(
                      serviceName, indexWarmingQueriesResource)
                  + warmingQueriesVersionId,
              bucketName,
              snapshotIndexDataRoot + IncrementalCommandUtils.SNAPSHOT_WARMING_QUERIES);
    } else {
      System.out.println("No warming queries present for index, skipping copy");
    }
  }

  private void writeMetadataFile(
      AmazonS3 s3Client, String bucketName, String metadataFileKey, SnapshotMetadata metadata)
      throws IOException {
    String metadataFileStr = OBJECT_MAPPER.writeValueAsString(metadata);
    System.out.println(
        "Writing metadata file key: " + metadataFileKey + ", content: " + metadataFileStr);
    byte[] fileData = StateUtils.toUTF8(metadataFileStr);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(fileData.length);
    s3Client.putObject(
        new PutObjectRequest(
            bucketName, metadataFileKey, new ByteArrayInputStream(fileData), objectMetadata));
  }
}
