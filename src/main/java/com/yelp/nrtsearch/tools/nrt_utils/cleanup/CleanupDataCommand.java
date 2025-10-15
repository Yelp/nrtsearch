/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.cleanup;

import static com.yelp.nrtsearch.tools.nrt_utils.backup.BackupCommandUtils.deleteObjects;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.tools.nrt_utils.backup.BackupCommandUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;

@CommandLine.Command(
    name = CleanupDataCommand.CLEANUP_DATA,
    description = "Cleanup unneeded index data in S3")
public class CleanupDataCommand implements Callable<Integer> {
  public static final String CLEANUP_DATA = "cleanupData";
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
      names = {"-d", "--deleteAfter"},
      description =
          "Delete unneeded files older than this, in the form <#><unit> "
              + "with valid units (s)econds, (m)inutes, (h)ours, (d)ays. (60m, 7h, 3d, etc.)",
      required = true)
  private String deleteAfter;

  @CommandLine.Option(
      names = {"--gracePeriod"},
      description =
          "Keep files within this grace period from the oldest index version creation, in the form <#><unit> "
              + "with valid units (s)econds, (m)inutes, (h)ours, (d)ays. (60m, 7h, 3d, etc.) default: ${DEFAULT-VALUE}",
      defaultValue = "3h")
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
    long deleteAfterMs = BackupCommandUtils.getTimeIntervalMs(deleteAfter);
    long gracePeriodMs = BackupCommandUtils.getTimeIntervalMs(gracePeriod);

    if (s3Client == null) {
      s3Client =
          StateCommandUtils.createS3Client(bucketName, region, credsFile, credsProfile, maxRetry);
    }
    S3Backend s3Backend = new S3Backend(bucketName, false, false, s3Client);

    String resolvedIndexResource =
        StateCommandUtils.getResourceName(s3Backend, serviceName, indexName, exactResourceName);

    // Cleanup point state files
    String pointStatePrefix =
        S3Backend.getIndexResourcePrefix(
            serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.POINT_STATE);
    if (!s3Backend.exists(
        serviceName, resolvedIndexResource, RemoteBackend.IndexResourceType.POINT_STATE)) {
      System.out.println("No data found for index: " + indexName);
      return 1;
    }
    String currentPointStateVersion = s3Backend.getCurrentResourceName(pointStatePrefix);
    long currentPointStateTimestampMs = validatePSNameAndGetTimestampMs(currentPointStateVersion);
    System.out.println(
        "Current index point state version: "
            + currentPointStateVersion
            + ", timestamp: "
            + currentPointStateTimestampMs);

    long currentTimeMs = System.currentTimeMillis();
    long minPointStateTimestampMs = currentTimeMs - deleteAfterMs;
    minPointStateTimestampMs = Math.min(minPointStateTimestampMs, currentPointStateTimestampMs);
    System.out.println(
        "Cleaning up version files, minPointStateTimestampMs: " + minPointStateTimestampMs);

    PointStateDeletionDecider pointStateDeletionDecider =
        new PointStateDeletionDecider(minPointStateTimestampMs);
    cleanupS3Files(s3Client, bucketName, pointStatePrefix, pointStateDeletionDecider, dryRun);

    String oldestRetainedPointFile = pointStateDeletionDecider.getOldestRetainedFile();
    System.out.println("Oldest point state version retained: " + oldestRetainedPointFile);
    if (oldestRetainedPointFile == null) {
      System.out.println("Could not determine oldest retained point state file");
      return 1;
    }

    // find the min of current time, current point state time, and lowest point state
    // time. This conservatively determines the lower bounds, in case there is an issue with
    // one of the timestamps
    long lowestVersionTimestampMs = validatePSNameAndGetTimestampMs(oldestRetainedPointFile);
    // subtract grace period for safety, this will be more important when there is pre copied
    // merge data in S3
    long dataMinTimestampMs =
        Math.min(Math.min(currentPointStateTimestampMs, currentTimeMs), lowestVersionTimestampMs)
            - gracePeriodMs;

    // get all the S3 files referenced by the first and last retained point state versions
    byte[] currentPointStateData =
        s3Client
            .getObject(bucketName, pointStatePrefix + currentPointStateVersion)
            .getObjectContent()
            .readAllBytes();
    NrtPointState currentPointState = RemoteUtils.pointStateFromUtf8(currentPointStateData);
    byte[] oldestPointStateData =
        s3Client
            .getObject(bucketName, pointStatePrefix + oldestRetainedPointFile)
            .getObjectContent()
            .readAllBytes();
    NrtPointState oldestPointState = RemoteUtils.pointStateFromUtf8(oldestPointStateData);

    Set<String> currentPointStateFiles = getPointStateFiles(currentPointState);
    Set<String> oldestPointStateFiles = getPointStateFiles(oldestPointState);

    // Cleanup index data files
    String dataPrefix = S3Backend.getIndexDataPrefix(serviceName, resolvedIndexResource);
    // uses union of current version and lowest version, this is done to conservatively
    // protect the lowest version index files to ensure it can be restored
    Set<String> activeIndexFiles = Sets.union(currentPointStateFiles, oldestPointStateFiles);
    System.out.println(
        "Cleaning up index data files, minTimestampMs: "
            + dataMinTimestampMs
            + ", activeIndexFiles: "
            + activeIndexFiles);

    // clean up all data files that are not needed by the retained index versions
    cleanupS3Files(
        s3Client,
        bucketName,
        dataPrefix,
        new IndexDataDeletionDecider(dataMinTimestampMs, activeIndexFiles),
        dryRun);
    return 0;
  }

  /**
   * Validate that the point state file name conforms to the expected format and return the
   * timestamp component in milliseconds.
   *
   * @param pointStateFileName point state file name
   * @return timestamp in milliseconds
   */
  @VisibleForTesting
  static long validatePSNameAndGetTimestampMs(String pointStateFileName) {
    String[] parts = pointStateFileName.split("-");
    if (parts.length != 7) {
      throw new IllegalArgumentException("Invalid point state name: " + pointStateFileName);
    }
    try {
      // parse file name components to validate
      String uuidString = String.join("-", parts[1], parts[2], parts[3], parts[4], parts[5]);
      UUID.fromString(uuidString);
      Long.valueOf(parts[6]);
      String timeString = pointStateFileName.split("-")[0];
      return TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid point state name: " + pointStateFileName, e);
    }
  }

  /**
   * Validate that the data file name conforms to the expected format and return the timestamp
   * component in milliseconds.
   *
   * @param dataFileName data file name
   * @return timestamp in milliseconds
   */
  @VisibleForTesting
  static long validateDataNameAndGetTimestampMs(String dataFileName) {
    String[] parts = dataFileName.split("-");
    // lucene index files start with '_'
    if (parts.length != 7 || !parts[6].startsWith("_")) {
      throw new IllegalArgumentException("Invalid data name: " + dataFileName);
    }
    try {
      // parse file name components to validate
      String uuidString = String.join("-", parts[1], parts[2], parts[3], parts[4], parts[5]);
      UUID.fromString(uuidString);
      String timeString = dataFileName.split("-")[0];
      return TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid data name: " + dataFileName, e);
    }
  }

  /**
   * Get the set of S3 index file names referenced by the point state.
   *
   * @param pointState point state
   * @return set of S3 index file names
   */
  private static Set<String> getPointStateFiles(NrtPointState pointState) {
    return pointState.files.entrySet().stream()
        .map(e -> S3Backend.getIndexBackendFileName(e.getKey(), e.getValue()))
        .collect(Collectors.toSet());
  }

  /** Decider for deleting index data files based on the min timestamp and active index files. */
  static class IndexDataDeletionDecider implements FileDeletionDecider {
    private final long minTimestampMs;
    private final Set<String> activeIndexFiles;
    private boolean done = false;

    IndexDataDeletionDecider(long minTimestampMs, Set<String> activeIndexFiles) {
      this.minTimestampMs = minTimestampMs;
      this.activeIndexFiles = activeIndexFiles;
    }

    @Override
    public boolean shouldDelete(String fileBaseName, long timestampMs) {
      if (activeIndexFiles.contains(fileBaseName)) {
        return false;
      }
      long fileNameTimestampMs = validateDataNameAndGetTimestampMs(fileBaseName);
      // once we hit the first time string that is newer than the min timestamp, we are done
      // since the files are sorted by time string
      if (fileNameTimestampMs >= minTimestampMs) {
        done = true;
        return false;
      }
      // extra sanity check to only delete files if both the file time string and the s3 timestamp
      // are older than the min timestamp
      return timestampMs < minTimestampMs;
    }

    @Override
    public boolean isDone() {
      return done;
    }
  }

  /**
   * Decider for deleting point state files based on the min timestamp. This decider will retain the
   * first file that is newer than the min timestamp.
   */
  static class PointStateDeletionDecider implements FileDeletionDecider {
    private final long minPointStateTimestampMs;
    private String oldestRetainedFile = null;
    private boolean done = false;

    PointStateDeletionDecider(long minPointStateTimestampMs) {
      this.minPointStateTimestampMs = minPointStateTimestampMs;
    }

    @Override
    public boolean shouldDelete(String fileBaseName, long timestampMs) {
      if (S3Backend.CURRENT_VERSION.equals(fileBaseName)) {
        return false;
      }
      long fileNameTimestampMs = validatePSNameAndGetTimestampMs(fileBaseName);
      // once we hit the first time string that is newer than the min timestamp, we are done
      // since the files are sorted by time string
      if (fileNameTimestampMs >= minPointStateTimestampMs) {
        oldestRetainedFile = fileBaseName;
        done = true;
        return false;
      }
      // extra sanity check to only delete files if both the file time string and the s3 timestamp
      // are older than the min timestamp
      return timestampMs < minPointStateTimestampMs;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    public String getOldestRetainedFile() {
      return oldestRetainedFile;
    }
  }

  /**
   * Interface for deciding if an object should be deleted from s3 given its base name and
   * modification time.
   */
  interface FileDeletionDecider {

    /**
     * Determine if an object should be deleted
     *
     * @param fileBaseName file name after the key prefix
     * @param timestampMs modification timestamp
     * @return if object should be deleted
     */
    boolean shouldDelete(String fileBaseName, long timestampMs);

    /**
     * Determine if the deletion process is done. This can be used to short circuit the cleanup
     * process when all subsequent files will be retained.
     *
     * @return if the deletion process is done
     */
    boolean isDone();
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
        if (deletionDecider.isDone()) {
          break;
        }
      }
      if (deletionDecider.isDone()) {
        break;
      }
      String token = result.getNextContinuationToken();
      req.setContinuationToken(token);
    } while (result.isTruncated());

    if (!deleteList.isEmpty() && !dryRun) {
      deleteObjects(s3Client, bucketName, deleteList);
    }
  }
}
