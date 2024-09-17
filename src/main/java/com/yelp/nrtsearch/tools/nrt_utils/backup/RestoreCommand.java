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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.TimeStringUtil;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;

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
    S3Backend s3Backend = new S3Backend(bucketName, false, s3Client);

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
    S3Object pointStateObject =
        s3Client.getObject(bucketName, snapshotDataRoot + BackupCommandUtils.SNAPSHOT_POINT_STATE);
    byte[] pointStateBytes = IOUtils.toByteArray(pointStateObject.getObjectContent());
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
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(pointStateBytes.length);
    s3Client.putObject(
        bucketName,
        restorePointStatePrefix + pointStateFileName,
        new ByteArrayInputStream(pointStateBytes),
        objectMetadata);

    System.out.println("Restoring index data: " + backendIndexFiles);

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(copyThreads);
    TransferManager transferManager =
        TransferManagerBuilder.standard()
            .withS3Client(s3Client)
            .withExecutorFactory(() -> executor)
            .withShutDownThreadPools(true)
            .build();
    try {
      List<Copy> copyJobs = new ArrayList<>();
      for (String fileName : backendIndexFiles) {
        CopyObjectRequest copyObjectRequest =
            new CopyObjectRequest(
                bucketName, snapshotDataRoot + fileName, bucketName, restoreDataPrefix + fileName);
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
    s3Client.copyObject(
        bucketName,
        snapshotDataRoot + BackupCommandUtils.SNAPSHOT_INDEX_STATE,
        bucketName,
        restoreIndexStatePrefix + stateFileName);
    s3Backend.setCurrentResource(restoreIndexStatePrefix, stateFileName);
  }

  private void maybeRestoreWarmingQueries(
      S3Backend s3Backend, String restoreIndexResource, String snapshotDataRoot) {
    String snapshotWarmingQueriesKey =
        snapshotDataRoot + BackupCommandUtils.SNAPSHOT_WARMING_QUERIES;
    if (!s3Client.doesObjectExist(bucketName, snapshotWarmingQueriesKey)) {
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
    s3Client.copyObject(
        bucketName,
        snapshotWarmingQueriesKey,
        bucketName,
        restoreWarmingQueriesPrefix + warmingQueriesFilename);
    s3Backend.setCurrentResource(restoreWarmingQueriesPrefix, warmingQueriesFilename);
  }

  private SnapshotMetadata loadMetadata(AmazonS3 s3Client, String snapshotRoot) throws IOException {
    String metadataFileKey =
        BackupCommandUtils.getSnapshotIndexMetadataKey(
            snapshotRoot, snapshotIndexIdentifier, snapshotTimeString);
    if (!s3Client.doesObjectExist(bucketName, metadataFileKey)) {
      throw new IllegalArgumentException("Metadata file does not exist: " + metadataFileKey);
    }
    System.out.println("Loading metadata key: " + metadataFileKey);
    S3Object stateObject = s3Client.getObject(bucketName, metadataFileKey);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(stateObject.getObjectContent(), byteArrayOutputStream);
    String fileContent = fromUTF8(byteArrayOutputStream.toByteArray());
    System.out.println("Contents: " + fileContent);
    return OBJECT_MAPPER.readValue(fileContent, SnapshotMetadata.class);
  }

  private void checkRestoreIndexNotExists(AmazonS3 s3Client, String restoreIndexResource) {
    String restorePrefix = restoreServiceName + "/" + restoreIndexResource;
    ListObjectsV2Result result = s3Client.listObjectsV2(bucketName, restorePrefix);
    List<S3ObjectSummary> objects = result.getObjectSummaries();
    if (!objects.isEmpty()) {
      throw new IllegalArgumentException(
          "Data exists at restore location, found: " + objects.get(0).getKey());
    }
  }

  private String getRestoreIndexResource() {
    return BackendGlobalState.getUniqueIndexName(restoreIndexName, getRestoreIndexId());
  }

  private String getRestoreIndexId() {
    if (restoreIndexId == null) {
      return TimeStringUtil.generateTimeStringMs();
    } else {
      if (!TimeStringUtil.isTimeStringMs(restoreIndexId)) {
        throw new IllegalStateException(
            "restoreIndexId must be a time string of the form yyyyMMddHHmmssSSS, got: "
                + restoreIndexId);
      }
      return restoreIndexId;
    }
  }
}
