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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.time.Instant;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = ListIncrementalSnapshotsCommand.LIST_INCREMENTAL_SNAPSHOTS,
    description = "List snapshots of incremental index data.")
public class ListIncrementalSnapshotsCommand implements Callable<Integer> {
  public static final String LIST_INCREMENTAL_SNAPSHOTS = "listIncrementalSnapshots";

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of nrtsearch cluster, either this on snapshotRoot must be specified")
  private String serviceName;

  @CommandLine.Option(
      names = {"-i", "--indexPrefix"},
      description = "Index prefix to list",
      defaultValue = "")
  private String indexPrefix;

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
      description =
          "Root s3 snapshot path, defaults to <serviceName>/snapshots/ either this or serviceName must be specified")
  private String snapshotRoot;

  @CommandLine.Option(
      names = {"--maxRetry"},
      description = "Maximum number of retry attempts for S3 failed requests",
      defaultValue = "20")
  private int maxRetry;

  @Override
  public Integer call() throws Exception {
    AmazonS3 s3Client =
        StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);

    String resolvedSnapshotRoot =
        IncrementalCommandUtils.getSnapshotRoot(snapshotRoot, serviceName);
    String listKeyPrefix = getIndexMetadataKeyPrefix(resolvedSnapshotRoot);
    listSnapshots(s3Client, bucketName, resolvedSnapshotRoot, listKeyPrefix);
    return 0;
  }

  static void listSnapshots(
      AmazonS3 s3Client, String bucketName, String snapshotRoot, String keyPrefix) {
    ListObjectsV2Request req =
        new ListObjectsV2Request().withBucketName(bucketName).withPrefix(keyPrefix);
    ListObjectsV2Result result;
    String metadataPrefix = getMetadataKeyPrefix(snapshotRoot);

    System.out.println("Listing snapshots for prefix: " + keyPrefix);
    do {
      result = s3Client.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        String snapshotSuffix = objectSummary.getKey().split(metadataPrefix)[1];
        String timestampStr = getTimestampStr(snapshotSuffix);
        String outputSuffix = timestampStr == null ? "" : " (" + timestampStr + ")";
        System.out.println(snapshotSuffix + outputSuffix);
      }
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());
  }

  static String getTimestampStr(String snapshotSuffix) {
    String[] splits = snapshotSuffix.split("/");
    if (splits.length != 2) {
      return null;
    }
    String timestampStr = splits[1];
    long timestampMs;
    try {
      timestampMs = Long.parseLong(timestampStr);
    } catch (NumberFormatException e) {
      return null;
    }
    return Instant.ofEpochMilli(timestampMs).toString();
  }

  static String getMetadataKeyPrefix(String snapshotRoot) {
    return snapshotRoot + IncrementalCommandUtils.METADATA_DIR + "/";
  }

  String getIndexMetadataKeyPrefix(String snapshotRoot) {
    return getMetadataKeyPrefix(snapshotRoot) + indexPrefix;
  }
}
