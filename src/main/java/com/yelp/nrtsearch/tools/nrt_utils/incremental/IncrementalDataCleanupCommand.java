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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(
    name = IncrementalDataCleanupCommand.INC_DATA_CLEANUP,
    description = "Delete incremental backup index files in S3 based on given criteria")
public class IncrementalDataCleanupCommand implements Callable<Integer> {
  private static final int DELETE_BATCH_SIZE = 1000;
  public static final String LATEST_VERSION_FILE = "_latest_version";
  public static final String INC_DATA_CLEANUP = "incrementalDataCleanup";

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
      names = {"-d", "--deleteAfter"},
      description =
          "Delete unneeded files older than this, in the form <#><unit> "
              + "with valid units (s)econds, (m)inutes, (h)ours, (d)ays. (60m, 7h, 3d, etc.)",
      required = true)
  private String deleteAfter;

  @CommandLine.Option(
      names = {"--minVersions"},
      description =
          "Minimum number of index versions to keep, regardless of age. default: ${DEFAULT-VALUE}",
      defaultValue = "5")
  private int minVersions;

  @CommandLine.Option(
      names = {"--gracePeriod"},
      description =
          "Keep files within this grace period from the oldest index version creation, in the form <#><unit> "
              + "with valid units (s)econds, (m)inutes, (h)ours, (d)ays. (60m, 7h, 3d, etc.) default: ${DEFAULT-VALUE}",
      defaultValue = "6h")
  private String gracePeriod;

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
    if (minVersions <= 0) {
      throw new IllegalArgumentException("minVersions must be > 0");
    }

    long deleteAfterMs = getTimeIntervalMs(deleteAfter);
    long gracePeriodMs = getTimeIntervalMs(gracePeriod);

    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    VersionManager versionManager = new VersionManager(s3Client, bucketName);

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(
            versionManager, serviceName, indexName, exactResourceName);
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(resolvedIndexResource);
    long currentVersion = versionManager.getLatestVersionNumber(serviceName, indexDataResource);
    System.out.println("Current index version: " + currentVersion);
    if (currentVersion < 0) {
      System.out.println("No data found for index: " + indexDataResource);
      return 1;
    }

    long minVersion = getMinRetainedVersion(currentVersion, minVersions);
    long currentTimeMs = System.currentTimeMillis();
    long minVersionTimestampMs = currentTimeMs - deleteAfterMs;
    System.out.println(
        "Cleaning up version files, minVersion: "
            + minVersion
            + ", minTimestampMs: "
            + minVersionTimestampMs);

    // remove all version number files that are older than deleteAfter and are not needed to
    // maintain the minimum number of versions
    String versionKeyPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(serviceName, indexDataResource);
    VersionFileDeletionDecider versionFileDeletionDecider =
        new VersionFileDeletionDecider(minVersion, minVersionTimestampMs);
    cleanupS3Files(s3Client, bucketName, versionKeyPrefix, versionFileDeletionDecider, dryRun);

    // the actual version file for minVersion may not exist, as it may have been removed
    // by a previous cleanup. Use the smallest version that was retained during the
    // cleanup pass
    long lowestRetainedVersion = versionFileDeletionDecider.getLowestRetainedVersion();
    System.out.println("Lowest version retained: " + lowestRetainedVersion);
    if (lowestRetainedVersion == Long.MAX_VALUE) {
      System.out.println("Could not determine lowest retained version");
      return 1;
    }

    // find the min of current time, current version update time, and lowest version update
    // time. This conservatively determines the lower bounds, in case there is an issue with
    // one of the timestamps
    long currentVersionTimestampMs =
        getVersionTimestampMs(s3Client, bucketName, serviceName, indexDataResource, currentVersion);
    long lowestVersionTimestampMs =
        getVersionTimestampMs(
            s3Client, bucketName, serviceName, indexDataResource, lowestRetainedVersion);
    long dataMinTimestampMs =
        Math.min(Math.min(currentVersionTimestampMs, currentTimeMs), lowestVersionTimestampMs)
            - gracePeriodMs;

    Set<String> currentFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client,
            bucketName,
            serviceName,
            indexDataResource,
            versionManager.getVersionString(
                serviceName, indexDataResource, String.valueOf(currentVersion)));
    Set<String> lowestVersionFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client,
            bucketName,
            serviceName,
            indexDataResource,
            versionManager.getVersionString(
                serviceName, indexDataResource, String.valueOf(lowestRetainedVersion)));

    String dataKeyPrefix = IncrementalCommandUtils.getDataKeyPrefix(serviceName, indexDataResource);
    // uses union of current version and lowest version, this is done to conservatively
    // protect the lowest version index files to ensure it can be restored
    Set<String> activeIndexFiles = Sets.union(currentFiles, lowestVersionFiles);
    System.out.println(
        "Cleaning up index data files, minTimestampMs: "
            + dataMinTimestampMs
            + ", activeIndexFiles: "
            + activeIndexFiles);

    // clean up all data files that are not needed by the retained index versions and all
    // manifest files that are past the grace period
    cleanupS3Files(
        s3Client,
        bucketName,
        dataKeyPrefix,
        new DataFileDeletionDecider(dataMinTimestampMs, activeIndexFiles),
        dryRun);

    return 0;
  }

  static class VersionFileDeletionDecider implements FileDeletionDecider {
    private final long minVersion;
    private final long minTimestampMs;
    private long lowestRetainedVersion = Long.MAX_VALUE;

    /**
     * Constructor.
     *
     * @param minVersion minimum index version to keep
     * @param minTimestampMs minimum modification timestamp to keep
     */
    VersionFileDeletionDecider(long minVersion, long minTimestampMs) {
      this.minVersion = minVersion;
      this.minTimestampMs = minTimestampMs;
    }

    /** Get smallest index data version not deleted. */
    public long getLowestRetainedVersion() {
      return lowestRetainedVersion;
    }

    @Override
    public boolean shouldDelete(String fileBaseName, long timestampMs) {
      if (LATEST_VERSION_FILE.equals(fileBaseName)) {
        return false;
      }
      long indexVersion = Long.parseLong(fileBaseName);
      if (indexVersion < minVersion && timestampMs < minTimestampMs) {
        return true;
      } else {
        if (indexVersion < lowestRetainedVersion) {
          lowestRetainedVersion = indexVersion;
        }
        return false;
      }
    }
  }

  static class DataFileDeletionDecider implements FileDeletionDecider {
    private final long minTimestampMs;
    private final Set<String> activeIndexFiles;

    /**
     * Constructor.
     *
     * @param minTimestampMs minimum timestamp of files to keep
     * @param activeIndexFiles index files currently in use
     */
    DataFileDeletionDecider(long minTimestampMs, Set<String> activeIndexFiles) {
      this.minTimestampMs = minTimestampMs;
      this.activeIndexFiles = activeIndexFiles;
    }

    @Override
    public boolean shouldDelete(String fileBaseName, long timestampMs) {
      if (timestampMs < minTimestampMs) {
        if (IncrementalCommandUtils.isDataFile(fileBaseName)) {
          return !activeIndexFiles.contains(fileBaseName);
        } else if (IncrementalCommandUtils.isManifestFile(fileBaseName)) {
          return true;
        } else {
          throw new IllegalArgumentException("Cannot classify file: " + fileBaseName);
        }
      }
      return false;
    }
  }

  /**
   * Interface for deciding if an object should be deleted from s3 given its base name and
   * modification time.
   */
  @FunctionalInterface
  interface FileDeletionDecider {

    /**
     * Determine if an object should be deleted
     *
     * @param fileBaseName file name after the key prefix
     * @param timestampMs modification timestamp
     * @return if object should be deleted
     */
    boolean shouldDelete(String fileBaseName, long timestampMs);
  }

  /**
   * Clean up files in s3. Checks all keys matching the given prefix, and uses the given {@link
   * FileDeletionDecider} to determine if they should be deleted.
   *
   * @param s3Client s3 client
   * @param bucketName s3 bucket name
   * @param keyPrefix key prefix to clean up
   * @param deletionDecider deletion decider
   * @param dryRun skip sending actual deletion requests to s3
   */
  static void cleanupS3Files(
      AmazonS3 s3Client,
      String bucketName,
      String keyPrefix,
      FileDeletionDecider deletionDecider,
      boolean dryRun) {
    ListObjectsV2Request req =
        new ListObjectsV2Request().withBucketName(bucketName).withPrefix(keyPrefix);
    ListObjectsV2Result result;

    List<String> deleteList = new ArrayList<>(DELETE_BATCH_SIZE);

    do {
      result = s3Client.listObjectsV2(req);

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        String objFileName = objectSummary.getKey().split(keyPrefix)[1];
        long versionTimestampMs = objectSummary.getLastModified().getTime();
        if (deletionDecider.shouldDelete(objFileName, versionTimestampMs)) {
          System.out.println(
              "Deleting object - key: "
                  + objectSummary.getKey()
                  + ", timestampMs: "
                  + versionTimestampMs);
          deleteList.add(objectSummary.getKey());
          if (deleteList.size() == DELETE_BATCH_SIZE) {
            if (!dryRun) {
              deleteObjects(s3Client, bucketName, deleteList);
            }
            deleteList.clear();
          }
        }
      }
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());

    if (!deleteList.isEmpty() && !dryRun) {
      deleteObjects(s3Client, bucketName, deleteList);
    }
  }

  /**
   * Delete a list of keys from s3, with a bulk delete request.
   *
   * @param s3Client s3 client
   * @param bucketName s3 bucket
   * @param keys keys to delete
   */
  static void deleteObjects(AmazonS3 s3Client, String bucketName, List<String> keys) {
    System.out.println("Batch deleting objects, size: " + keys.size());
    DeleteObjectsRequest multiObjectDeleteRequest =
        new DeleteObjectsRequest(bucketName).withKeys(keys.toArray(new String[0])).withQuiet(true);
    s3Client.deleteObjects(multiObjectDeleteRequest);
  }

  /**
   * Get the modification timestamp for a given index data version object.
   *
   * @param s3Client s3 client
   * @param bucketName s3 bucket
   * @param serviceName nrtsearch cluster service name
   * @param indexResource index data resource name
   * @param version index data version
   * @return last modified time of version file
   */
  static long getVersionTimestampMs(
      AmazonS3 s3Client,
      String bucketName,
      String serviceName,
      String indexResource,
      long version) {
    String versionPath = String.format("%s/_version/%s/%s", serviceName, indexResource, version);
    ObjectMetadata metadata = s3Client.getObjectMetadata(bucketName, versionPath);
    return metadata.getLastModified().getTime();
  }

  /**
   * Get the minimum index data version to retain during cleanup.
   *
   * @param currentVersion current data version
   * @param minVersions minimum versions to retain
   * @return minimum retained version
   */
  static long getMinRetainedVersion(long currentVersion, int minVersions) {
    return Math.min(Math.max(0L, currentVersion - minVersions + 1), currentVersion);
  }

  /**
   * Parse an interval string into a numeric interval in ms. The string must be in a form 10s, 5h,
   * etc. Numeric component must be positive. The units component must be one of (s)econds,
   * (m)inutes, (h)ours, (d)ays.
   *
   * @param interval interval string
   * @return interval in ms
   */
  static long getTimeIntervalMs(String interval) {
    String trimmed = interval.trim();
    if (trimmed.length() < 2) {
      throw new IllegalArgumentException("Invalid time interval: " + trimmed);
    }
    char endChar = trimmed.charAt(trimmed.length() - 1);
    long numberVal = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));

    if (numberVal < 1) {
      throw new IllegalArgumentException("Time interval must be > 0");
    }

    switch (endChar) {
      case 's':
        return TimeUnit.SECONDS.toMillis(numberVal);
      case 'm':
        return TimeUnit.MINUTES.toMillis(numberVal);
      case 'h':
        return TimeUnit.HOURS.toMillis(numberVal);
      case 'd':
        return TimeUnit.DAYS.toMillis(numberVal);
      default:
        throw new IllegalArgumentException("Unknown time unit: " + endChar);
    }
  }
}
