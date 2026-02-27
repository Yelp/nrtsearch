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

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.fromUTF8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.Copy;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;

@CommandLine.Command(
    name = RestoreIncrementalCommand.RESTORE_INCREMENTAL,
    description =
        "Restore snapshot of incremental index data into existing cluster. Legacy command for use with v0 cluster data.")
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

  private S3Client s3Client;
  private software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient;
  private software.amazon.awssdk.transfer.s3.S3TransferManager transferManager;

  @VisibleForTesting
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @VisibleForTesting
  void setS3AsyncClient(software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
  }

  @VisibleForTesting
  void setTransferManager(software.amazon.awssdk.transfer.s3.S3TransferManager transferManager) {
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
      S3Client s3Client,
      LegacyVersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot)
      throws IOException, InterruptedException {
    String restoreIndexDataResource =
        IncrementalCommandUtils.getIndexDataResource(restoreIndexResource);
    String fileListId = UUID.randomUUID().toString();
    String restoreDataKeyPrefix = restoreRoot + restoreIndexDataResource + "/";

    System.out.println("Restoring index data to version key: " + restoreDataKeyPrefix + fileListId);
    CopyObjectRequest copyRequest1 =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_FILES)
            .destinationBucket(bucketName)
            .destinationKey(restoreDataKeyPrefix + fileListId)
            .build();
    s3Client.copyObject(copyRequest1);

    Set<String> indexFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client, bucketName, restoreServiceName, restoreIndexDataResource, fileListId);
    System.out.println("Restoring index data: " + indexFiles);

    software.amazon.awssdk.transfer.s3.S3TransferManager transferManagerToUse =
        this.transferManager;
    boolean shouldCloseTransferManager = false;
    if (transferManagerToUse == null) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(copyThreads);
      software.amazon.awssdk.services.s3.S3AsyncClient s3AsyncClient =
          software.amazon.awssdk.services.s3.S3AsyncClient.crtBuilder()
              .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider())
              .region(s3Client.serviceClientConfiguration().region())
              .build();
      transferManagerToUse =
          software.amazon.awssdk.transfer.s3.S3TransferManager.builder()
              .s3Client(s3AsyncClient)
              .build();
      shouldCloseTransferManager = true;
    }
    try {
      List<Copy> copyJobs = new ArrayList<>();
      for (String fileName : indexFiles) {
        CopyObjectRequest copyObjectRequest =
            CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(snapshotDataRoot + fileName)
                .destinationBucket(bucketName)
                .destinationKey(restoreDataKeyPrefix + fileName)
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
    versionManager.blessVersion(restoreServiceName, restoreIndexDataResource, fileListId);
  }

  private void restoreIndexState(
      S3Client s3Client,
      LegacyVersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot)
      throws IOException {
    String restoreIndexStateResource =
        LegacyStateCommandUtils.getIndexStateResource(restoreIndexResource);
    String stateFileId = UUID.randomUUID().toString();
    String restoreStateKey = restoreRoot + restoreIndexStateResource + "/" + stateFileId;

    System.out.println("Restoring index state to key: " + restoreStateKey);
    CopyObjectRequest copyRequest2 =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_INDEX_STATE_FILE)
            .destinationBucket(bucketName)
            .destinationKey(restoreStateKey)
            .build();
    s3Client.copyObject(copyRequest2);
    versionManager.blessVersion(restoreServiceName, restoreIndexStateResource, stateFileId);
  }

  private void maybeRestoreWarmingQueries(
      S3Client s3Client,
      LegacyVersionManager versionManager,
      String restoreIndexResource,
      String restoreRoot,
      String snapshotDataRoot) {
    String snapshotWarmingQueriesKey =
        snapshotDataRoot + IncrementalCommandUtils.SNAPSHOT_WARMING_QUERIES;
    if (!objectExists(s3Client, bucketName, snapshotWarmingQueriesKey)) {
      System.out.println("Warming queries not present in snapshot, skipping");
      return;
    }
    String restoreWarmingQueriesResource =
        IncrementalCommandUtils.getWarmingQueriesResource(restoreIndexResource);
    String warmingQueriesFileId = UUID.randomUUID().toString();
    String restoreWarmingQueriesKey =
        restoreRoot + restoreWarmingQueriesResource + "/" + warmingQueriesFileId;

    System.out.println("Restoring warming queries to key: " + restoreWarmingQueriesKey);
    CopyObjectRequest copyRequest3 =
        CopyObjectRequest.builder()
            .sourceBucket(bucketName)
            .sourceKey(snapshotWarmingQueriesKey)
            .destinationBucket(bucketName)
            .destinationKey(restoreWarmingQueriesKey)
            .build();
    s3Client.copyObject(copyRequest3);
    versionManager.blessVersion(
        restoreServiceName, restoreWarmingQueriesResource, warmingQueriesFileId);
  }

  private SnapshotMetadata loadMetadata(S3Client s3Client, String snapshotRoot) throws IOException {
    String metadataFileKey =
        IncrementalCommandUtils.getSnapshotIndexMetadataKey(
            snapshotRoot, snapshotIndexIdentifier, snapshotTimestamp);
    if (!objectExists(s3Client, bucketName, metadataFileKey)) {
      throw new IllegalArgumentException("Metadata file does not exist: " + metadataFileKey);
    }
    System.out.println("Loading metadata key: " + metadataFileKey);
    GetObjectRequest getRequest =
        GetObjectRequest.builder().bucket(bucketName).key(metadataFileKey).build();
    ResponseInputStream<GetObjectResponse> stateObject = s3Client.getObject(getRequest);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(stateObject, byteArrayOutputStream);
    String fileContent = fromUTF8(byteArrayOutputStream.toByteArray());
    System.out.println("Contents: " + fileContent);
    return OBJECT_MAPPER.readValue(fileContent, SnapshotMetadata.class);
  }

  private void checkRestoreIndexNotExists(
      S3Client s3Client, String restoreRoot, String restoreIndexResource) {
    String restorePrefix = restoreRoot + restoreIndexResource;
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(restorePrefix).build();
    ListObjectsV2Response result = s3Client.listObjectsV2(listRequest);
    List<S3Object> objects = result.contents();
    if (!objects.isEmpty()) {
      throw new IllegalArgumentException(
          "Data exists at restore location, found: " + objects.get(0).key());
    }
  }

  private boolean objectExists(S3Client s3Client, String bucket, String key) {
    try {
      HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
      s3Client.headObject(request);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  private String getRestoreRoot() {
    return restoreServiceName + "/";
  }

  private String getRestoreIndexResource() {
    return LegacyStateCommandUtils.getUniqueIndexName(restoreIndexName, getRestoreIndexId());
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
