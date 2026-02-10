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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

@CommandLine.Command(
    name = CleanupSnapshotsCommand.CLEANUP_SNAPSHOTS,
    description = "Delete snapshots of index data.")
public class CleanupSnapshotsCommand implements Callable<Integer> {
  public static final String CLEANUP_SNAPSHOTS = "cleanupSnapshots";
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

  private S3Client s3Client;

  record TimestampAndTimeString(long timestampMs, String timeString) {}

  @VisibleForTesting
  void setS3Client(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public Integer call() throws Exception {
    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    S3Backend s3Backend = new S3Backend(bucketName, false, S3Backend.DEFAULT_CONFIG, s3Client);

    long deleteAfterMs = BackupCommandUtils.getTimeIntervalMs(deleteAfter);
    long minTimestampMs = System.currentTimeMillis() - deleteAfterMs;
    System.out.println("minTimestampMs: " + minTimestampMs);

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(s3Backend, serviceName, indexName, exactResourceName);
    String resolvedSnapshotRoot = BackupCommandUtils.getSnapshotRoot(snapshotRoot, serviceName);
    List<TimestampAndTimeString> snapshotTimestamps =
        getSnapshotTimestamps(s3Client, resolvedIndexResource, resolvedSnapshotRoot);
    System.out.println("Found metadata: " + snapshotTimestamps);

    if (minSnapshots > 0 && !snapshotTimestamps.isEmpty()) {
      // timestamps are sorted in natural order
      int index = Math.max(0, snapshotTimestamps.size() - minSnapshots);
      if (snapshotTimestamps.get(index).timestampMs < minTimestampMs) {
        minTimestampMs = snapshotTimestamps.get(index).timestampMs;
      }
      System.out.println("Adjusted min timestamp: " + minTimestampMs);
    }

    final long finalMinTimestampMs = minTimestampMs;
    List<TimestampAndTimeString> deleteTimestamps =
        snapshotTimestamps.stream()
            .filter(l -> l.timestampMs < finalMinTimestampMs)
            .collect(Collectors.toList());

    deleteSnapshotMetadata(s3Client, resolvedIndexResource, resolvedSnapshotRoot, deleteTimestamps);
    deleteSnapshotIndexData(s3Client, resolvedIndexResource, resolvedSnapshotRoot, minTimestampMs);

    return 0;
  }

  private void deleteSnapshotIndexData(
      S3Client s3Client,
      String resolvedIndexResource,
      String resolvedSnapshotRoot,
      long minTimestampMs) {
    String dataPrefix = getIndexSnapshotDataPrefix(resolvedSnapshotRoot, resolvedIndexResource);

    // Workaround: S3Mock (0.2.6) doesn't properly support commonPrefixes with AWS SDK v2
    // List all objects and manually extract unique directory prefixes
    ListObjectsV2Request.Builder reqBuilder =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(dataPrefix);
    ListObjectsV2Response result;
    Set<String> dataPrefixes = new HashSet<>();

    do {
      result = s3Client.listObjectsV2(reqBuilder.build());
      // Extract directory prefixes from object keys
      for (S3Object s3Object : result.contents()) {
        String key = s3Object.key();
        // Remove the base prefix to get the relative path
        if (key.startsWith(dataPrefix) && key.length() > dataPrefix.length()) {
          String relativePath = key.substring(dataPrefix.length());
          // Get the first directory component (timestamp directory)
          int slashIndex = relativePath.indexOf('/');
          if (slashIndex > 0) {
            String timeString = relativePath.substring(0, slashIndex);
            dataPrefixes.add(dataPrefix + timeString + "/");
          }
        }
      }
      String token = result.nextContinuationToken();
      reqBuilder.continuationToken(token);
    } while (result.isTruncated());

    List<String> deletePrefixes = new ArrayList<>();
    for (String prefix : dataPrefixes) {
      String[] splits = prefix.split("/");
      String timeString = splits[splits.length - 1];
      if (TimeStringUtils.isTimeStringMs(timeString)) {
        long dataTimestamp = TimeStringUtils.parseTimeStringMs(timeString).toEpochMilli();
        if (dataTimestamp < minTimestampMs) {
          deletePrefixes.add(prefix);
        }
      } else {
        System.out.println("Skipping invalid timestamp: " + timeString);
      }
    }
    deletePrefixes(s3Client, deletePrefixes);
  }

  private void deletePrefixes(S3Client s3Client, List<String> prefixes) {
    List<String> deleteList = new ArrayList<>(DELETE_BATCH_SIZE);
    for (String prefix : prefixes) {
      System.out.println("Deleting data prefix: " + prefix);
      ListObjectsV2Request.Builder reqBuilder =
          ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix);
      ListObjectsV2Response result;

      do {
        result = s3Client.listObjectsV2(reqBuilder.build());

        for (S3Object s3Object : result.contents()) {
          System.out.println("Delete: " + s3Object.key());
          deleteList.add(s3Object.key());
          if (deleteList.size() == DELETE_BATCH_SIZE) {
            if (!dryRun) {
              BackupCommandUtils.deleteObjects(s3Client, bucketName, deleteList);
            }
            deleteList.clear();
          }
        }
        String token = result.nextContinuationToken();
        reqBuilder.continuationToken(token);
      } while (result.isTruncated());
    }
    if (!deleteList.isEmpty() && !dryRun) {
      BackupCommandUtils.deleteObjects(s3Client, bucketName, deleteList);
    }
  }

  private void deleteSnapshotMetadata(
      S3Client s3Client,
      String resolvedIndexResource,
      String resolvedSnapshotRoot,
      List<TimestampAndTimeString> versions) {
    for (TimestampAndTimeString version : versions) {
      String key =
          BackupCommandUtils.getSnapshotIndexMetadataKey(
              resolvedSnapshotRoot, resolvedIndexResource, version.timeString);
      System.out.println("Deleting snapshot metadata: " + key);
      if (!dryRun) {
        DeleteObjectRequest deleteRequest =
            DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
        s3Client.deleteObject(deleteRequest);
      }
    }
  }

  private List<TimestampAndTimeString> getSnapshotTimestamps(
      S3Client s3Client, String resolvedIndexResource, String resolvedSnapshotRoot) {
    String metadataPrefix =
        BackupCommandUtils.getSnapshotIndexMetadataPrefix(
            resolvedSnapshotRoot, resolvedIndexResource);
    List<TimestampAndTimeString> snapshotTimestamps = new ArrayList<>();

    ListObjectsV2Request.Builder reqBuilder =
        ListObjectsV2Request.builder().bucket(bucketName).prefix(metadataPrefix);
    ListObjectsV2Response result;

    do {
      result = s3Client.listObjectsV2(reqBuilder.build());

      for (S3Object s3Object : result.contents()) {
        String snapshotSuffix = s3Object.key().split(metadataPrefix)[1];
        if (TimeStringUtils.isTimeStringMs(snapshotSuffix)) {
          long timestampMs = TimeStringUtils.parseTimeStringMs(snapshotSuffix).toEpochMilli();
          snapshotTimestamps.add(new TimestampAndTimeString(timestampMs, snapshotSuffix));
        } else {
          System.out.println("Skipping invalid timestamp: " + snapshotSuffix);
        }
      }
      String token = result.nextContinuationToken();
      reqBuilder.continuationToken(token);
    } while (result.isTruncated());

    snapshotTimestamps.sort(Comparator.comparingLong(e -> e.timestampMs));
    return snapshotTimestamps;
  }

  private String getIndexSnapshotDataPrefix(
      String resolvedSnapshotRoot, String resolvedIndexResource) {
    return resolvedSnapshotRoot + resolvedIndexResource + "/";
  }
}
