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
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;

@CommandLine.Command(
    name = RestoreIncrementalCommand.RESTORE_INCREMENTAL,
    description = "Restore snapshot of incremental index data into existing cluster")
public class RestoreIncrementalCommand implements Callable<Integer> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String RESTORE_INCREMENTAL = "restoreIncremental";

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
      description = "UUID to use for restored index, random if not specified")
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
      description = "File holding AWS credentials, uses default locations if not set")
  private String credsFile;

  @CommandLine.Option(
      names = {"-p", "--credsProfile"},
      description = "Profile to use from creds file",
      defaultValue = "default")
  private String credsProfile;

  @CommandLine.Option(
      names = {"--snapshotIndexIdentifier"},
      description = "Index identifier for snapshot, in the form <index_name>-<UUID>",
      required = true)
  private String snapshotIndexIdentifier;

  @CommandLine.Option(
      names = {"--snapshotTimestamp"},
      description = "Timestamp of snapshot data to restore",
      required = true)
  private long snapshotTimestamp;

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

    String resolvedSnapshotRoot =
        IncrementalCommandUtils.getSnapshotRoot(snapshotRoot, snapshotServiceName);
    String snapshotDataRoot =
        IncrementalCommandUtils.getSnapshotIndexDataRoot(
            resolvedSnapshotRoot, snapshotIndexIdentifier, snapshotTimestamp);
    String restoreRoot = getRestoreRoot();
    String restoreIndexResource = getRestoreIndexResource();

    System.out.println(
        "String restore of index, snapshotDataRoot: "
            + snapshotDataRoot
            + ", restoreRoot: "
            + restoreRoot
            + ", restoreIndex: "
            + restoreIndexResource);

    checkRestoreIndexNotExists(s3Client, restoreRoot, restoreIndexResource);

    SnapshotMetadata metadata = loadMetadata(s3Client, resolvedSnapshotRoot);
    restoreIndexData(s3Client, versionManager, restoreIndexResource, restoreRoot, snapshotDataRoot);
    restoreIndexState(
        s3Client, versionManager, restoreIndexResource, restoreRoot, snapshotDataRoot);
    maybeRestoreWarmingQueries(
        s3Client, versionManager, restoreIndexResource, restoreRoot, snapshotDataRoot);

    System.out.println("Restore completed");
    return 0;
  }

  private void restoreIndexData(
      AmazonS3 s3Client,
      VersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot)
      throws IOException, InterruptedException {
    String restoreIndexDataResource =
        IncrementalCommandUtils.getIndexDataResource(restoreIndexResource);
    String fileListId = UUID.randomUUID().toString();
    String restoreDataKeyPrefix = restoreRoot + restoreIndexDataResource + "/";

    System.out.println("Restoring index data to version key: " + restoreDataKeyPrefix + fileListId);
    s3Client.copyObject(
        bucketName,
        snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_FILES,
        bucketName,
        restoreDataKeyPrefix + fileListId);

    Set<String> indexFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client, bucketName, restoreServiceName, restoreIndexDataResource, fileListId);
    System.out.println("Restoring index data: " + indexFiles);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(copyThreads);
    TransferManager transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3Client)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(true)
            .build();
    try {
      List<Copy> copyJobs = new ArrayList<>();
      for (String fileName : indexFiles) {
        CopyObjectRequest copyObjectRequest =
            new CopyObjectRequest(
                bucketName,
                snapshotDataRoot + fileName,
                bucketName,
                restoreDataKeyPrefix + fileName);
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
    versionManager.blessVersion(restoreServiceName, restoreIndexDataResource, fileListId);
  }

  private void restoreIndexState(
      AmazonS3 s3Client,
      VersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot)
      throws IOException {
    String restoreIndexStateResource =
        StateCommandUtils.getIndexStateResource(restoreIndexResource);
    String stateFileId = UUID.randomUUID().toString();
    String restoreStateKey = restoreRoot + restoreIndexStateResource + "/" + stateFileId;

    System.out.println("Restoring index state to key: " + restoreStateKey);
    s3Client.copyObject(
        bucketName,
        snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_STATE_FILE,
        bucketName,
        restoreStateKey);
    versionManager.blessVersion(restoreServiceName, restoreIndexStateResource, stateFileId);
  }

  private void maybeRestoreWarmingQueries(
      AmazonS3 s3Client,
      VersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot) {
    String snapshotWarmingQueriesKey =
        snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_WARMING_QUERIES;
    if (!s3Client.doesObjectExist(bucketName, snapshotWarmingQueriesKey)) {
      System.out.println("Warming queries not present in snapshot, skipping");
      return;
    }
    String restoreWarmingQueriesResource =
        IncrementalCommandUtils.getWarmingQueriesResource(restoreIndexResource);
    String warmingQueriesFileId = UUID.randomUUID().toString();
    String restoreWarmingQueriesKey =
        restoreRoot + restoreWarmingQueriesResource + "/" + warmingQueriesFileId;

    System.out.println("Restoring warming queries to key: " + restoreWarmingQueriesKey);
    s3Client.copyObject(
        bucketName, snapshotWarmingQueriesKey, bucketName, restoreWarmingQueriesKey);
    versionManager.blessVersion(
        restoreServiceName, restoreWarmingQueriesResource, warmingQueriesFileId);
  }

  private SnapshotMetadata loadMetadata(AmazonS3 s3Client, String snapshotRoot) throws IOException {
    String metadataFileKey =
        IncrementalCommandUtils.getSnapshotIndexMetadataKey(
            snapshotRoot, snapshotIndexIdentifier, snapshotTimestamp);
    if (!s3Client.doesObjectExist(bucketName, metadataFileKey)) {
      throw new IllegalArgumentException("Metadata file does not exist: " + metadataFileKey);
    }
    System.out.println("Loading metadata key: " + metadataFileKey);
    S3Object stateObject = s3Client.getObject(bucketName, metadataFileKey);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(stateObject.getObjectContent(), byteArrayOutputStream);
    String fileContent = StateUtils.fromUTF8(byteArrayOutputStream.toByteArray());
    System.out.println("Contents: " + fileContent);
    return OBJECT_MAPPER.readValue(fileContent, SnapshotMetadata.class);
  }

  private void checkRestoreIndexNotExists(
      AmazonS3 s3Client, String restoreRoot, String restoreIndexResource) {
    String restorePrefix = restoreRoot + restoreIndexResource;
    ListObjectsV2Result result = s3Client.listObjectsV2(bucketName, restorePrefix);
    List<S3ObjectSummary> objects = result.getObjectSummaries();
    if (!objects.isEmpty()) {
      throw new IllegalArgumentException(
          "Data exists at restore location, found: " + objects.get(0).getKey());
    }
  }

  private String getRestoreRoot() {
    return restoreServiceName + "/";
  }

  private String getRestoreIndexResource() {
    return BackendGlobalState.getUniqueIndexName(restoreIndexName, getRestoreIndexId());
  }

  private String getRestoreIndexId() {
    if (restoreIndexId == null) {
      return UUID.randomUUID().toString();
    } else {
      if (!IncrementalCommandUtils.isUUID(restoreIndexId)) {
        throw new IllegalStateException("restoreIndexId must be a UUID");
      }
      return restoreIndexId;
    }
  }
}
