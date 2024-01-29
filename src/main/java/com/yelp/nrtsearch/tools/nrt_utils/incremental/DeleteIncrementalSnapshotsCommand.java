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

import static com.yelp.nrtsearch.tools.nrt_utils.incremental.IncrementalDataCleanupCommand.deleteObjects;
import static com.yelp.nrtsearch.tools.nrt_utils.incremental.IncrementalDataCleanupCommand.getTimeIntervalMs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;

@CommandLine.Command(
    name = DeleteIncrementalSnapshotsCommand.DELETE_INCREMENTAL_SNAPSHOTS,
    description = "Delete snapshots of incremental index data.")
public class DeleteIncrementalSnapshotsCommand implements Callable<Integer> {
  public static final String DELETE_INCREMENTAL_SNAPSHOTS = "deleteIncrementalSnapshots";
  private static final int DELETE_BATCH_SIZE = 1000;

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
      names = {"-d", "--deleteAfter"},
      description =
          "Delete snapshot data that is older than this, in the form <#><unit> "
              + "with valid units (s)econds, (m)inutes, (h)ours, (d)ays. (60m, 7h, 3d, etc.)",
      required = true)
  private String deleteAfter;

  @CommandLine.Option(
      names = {"--minSnapshots"},
      description =
          "Minimum number of latest snapshots to keep, regardless of age. default: ${DEFAULT-VALUE}",
      defaultValue = "1")
  private int minSnapshots;

  @CommandLine.Option(
      names = {"--dryRun"},
      description = "Print file deletions, instead of applying to S3")
  private boolean dryRun;

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

    long deleteAfterMs = getTimeIntervalMs(deleteAfter);
    long minTimestampMs = System.currentTimeMillis() - deleteAfterMs;
    System.out.println("minTimestampMs: " + minTimestampMs);

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(
            versionManager, serviceName, indexName, exactResourceName);
    String resolvedSnapshotRoot =
        IncrementalCommandUtils.getSnapshotRoot(snapshotRoot, serviceName);
    List<Long> snapshotTimestamps =
        getSnapshotTimestamps(s3Client, resolvedIndexResource, resolvedSnapshotRoot);
    System.out.println("Found metadata: " + snapshotTimestamps);

    if (minSnapshots > 0 && !snapshotTimestamps.isEmpty()) {
      // timestamps are sorted in natural order
      int index = Math.max(0, snapshotTimestamps.size() - minSnapshots);
      if (snapshotTimestamps.get(index) < minTimestampMs) {
        minTimestampMs = snapshotTimestamps.get(index);
      }
      System.out.println("Adjusted min timestamp: " + minTimestampMs);
    }

    final long finalMinTimestampMs = minTimestampMs;
    List<Long> deleteTimestamps =
        snapshotTimestamps.stream()
            .filter(l -> l < finalMinTimestampMs)
            .collect(Collectors.toList());

    deleteSnapshotMetadata(s3Client, resolvedIndexResource, resolvedSnapshotRoot, deleteTimestamps);
    deleteSnapshotIndexData(s3Client, resolvedIndexResource, resolvedSnapshotRoot, minTimestampMs);

    return 0;
  }

  private void deleteSnapshotIndexData(
      AmazonS3 s3Client,
      String resolvedIndexResource,
      String resolvedSnapshotRoot,
      long minTimestampMs) {
    String dataPrefix = getIndexSnapshotDataPrefix(resolvedSnapshotRoot, resolvedIndexResource);
    ListObjectsV2Request req =
        new ListObjectsV2Request()
            .withBucketName(bucketName)
            .withDelimiter("/")
            .withPrefix(dataPrefix);
    ListObjectsV2Result result = s3Client.listObjectsV2(req);
    List<String> dataPrefixes = result.getCommonPrefixes();
    System.out.println("Data prefixes: " + dataPrefixes);

    List<String> deletePrefixes = new ArrayList<>();
    for (String prefix : dataPrefixes) {
      String[] splits = prefix.split("/");
      long dataTimestamp = Long.parseLong(splits[splits.length - 1]);
      if (dataTimestamp < minTimestampMs) {
        deletePrefixes.add(prefix);
      }
    }
    deletePrefixes(s3Client, deletePrefixes);
  }

  private void deletePrefixes(AmazonS3 s3Client, List<String> prefixes) {
    List<String> deleteList = new ArrayList<>(DELETE_BATCH_SIZE);
    for (String prefix : prefixes) {
      System.out.println("Deleting data prefix: " + prefix);
      ListObjectsV2Request req =
          new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
      ListObjectsV2Result result;

      do {
        result = s3Client.listObjectsV2(req);

        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
          System.out.println("Delete: " + objectSummary.getKey());
          deleteList.add(objectSummary.getKey());
          if (deleteList.size() == DELETE_BATCH_SIZE) {
            if (!dryRun) {
              deleteObjects(s3Client, bucketName, deleteList);
            }
            deleteList.clear();
          }
        }
        String token = result.getNextContinuationToken();
        req.setContinuationToken(token);
      } while (result.isTruncated());
    }
    if (!deleteList.isEmpty() && !dryRun) {
      deleteObjects(s3Client, bucketName, deleteList);
    }
  }

  private void deleteSnapshotMetadata(
      AmazonS3 s3Client,
      String resolvedIndexResource,
      String resolvedSnapshotRoot,
      List<Long> versions) {
    for (Long version : versions) {
      String key =
          IncrementalCommandUtils.getSnapshotIndexMetadataKey(
              resolvedSnapshotRoot, resolvedIndexResource, version);
      System.out.println("Deleting snapshot metadata: " + key);
      if (!dryRun) {
        s3Client.deleteObject(bucketName, key);
      }
    }
  }

  private List<Long> getSnapshotTimestamps(
      AmazonS3 s3Client, String resolvedIndexResource, String resolvedSnapshotRoot) {
    String metadataPrefix =
        getIndexSnapshotMetadataPrefix(resolvedSnapshotRoot, resolvedIndexResource);
    List<Long> snapshotTimestamps = new ArrayList<>();

    ListObjectsV2Request req =
        new ListObjectsV2Request().withBucketName(bucketName).withPrefix(metadataPrefix);
    ListObjectsV2Result result;

    do {
      result = s3Client.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        String snapshotSuffix = objectSummary.getKey().split(metadataPrefix)[1];
        snapshotTimestamps.add(Long.parseLong(snapshotSuffix));
      }
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());

    snapshotTimestamps.sort(Comparator.comparingLong(e -> e));
    return snapshotTimestamps;
  }

  private String getIndexSnapshotMetadataPrefix(
      String resolvedSnapshotRoot, String resolvedIndexResource) {
    return resolvedSnapshotRoot
        + IncrementalCommandUtils.METADATA_DIR
        + "/"
        + resolvedIndexResource
        + "/";
  }

  private String getIndexSnapshotDataPrefix(
      String resolvedSnapshotRoot, String resolvedIndexResource) {
    return resolvedSnapshotRoot + resolvedIndexResource + "/";
  }
}
