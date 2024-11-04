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
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BackupCommandUtils {
  public static final String SNAPSHOT_INDEX_STATE = "index_state";
  public static final String SNAPSHOT_POINT_STATE = "index_point_state";
  public static final String SNAPSHOT_WARMING_QUERIES = "index_warming_queries";
  public static final String SNAPSHOT_DIR = "snapshots";
  public static final String METADATA_DIR = "metadata";

  private BackupCommandUtils() {}

  /**
   * Get the root S3 key for snapshots. If a snapshotRoot is provided, it will be used. Otherwise,
   * this defaults to serviceName/snapshots/
   *
   * @param snapshotRoot snapshot root key, or null
   * @param serviceName nrtsearch cluster service name
   * @return snapshot root key, with trailing slash
   */
  public static String getSnapshotRoot(String snapshotRoot, String serviceName) {
    if (snapshotRoot == null && serviceName == null) {
      throw new IllegalArgumentException("Must specify snapshotRoot or serviceName");
    }
    String root = snapshotRoot == null ? serviceName + "/" + SNAPSHOT_DIR + "/" : snapshotRoot;
    if (!root.endsWith("/")) {
      root += "/";
    }
    return root;
  }

  /**
   * Get the root key for index data for a specific snapshot time string.
   *
   * @param snapshotRoot root key for all snapshots
   * @param indexResource index resource (name-timeString)
   * @param timeStringMs snapshot time string of the form yyyyMMddHHmmssSSS
   * @return root key for snapshot data
   */
  public static String getSnapshotIndexDataRoot(
      String snapshotRoot, String indexResource, String timeStringMs) {
    return snapshotRoot + indexResource + "/" + timeStringMs + "/";
  }

  /**
   * Get the S3 key for metadata object for a specific snapshot time string.
   *
   * @param snapshotRoot root key for all snapshots
   * @param indexResource index resource (name-timeString)
   * @param timeStringMs snapshot time string of the form yyyyMMddHHmmssSSS
   * @return key for snapshot metadata
   */
  public static String getSnapshotIndexMetadataKey(
      String snapshotRoot, String indexResource, String timeStringMs) {
    return snapshotRoot + METADATA_DIR + "/" + indexResource + "/" + timeStringMs;
  }

  /**
   * Get the S3 key prefix for metadata objects for an index resource.
   *
   * @param snapshotRoot root key for all snapshots
   * @param indexResource index resource (name-timeString)
   * @return key prefix for snapshot metadata
   */
  public static String getSnapshotIndexMetadataPrefix(String snapshotRoot, String indexResource) {
    return snapshotRoot + METADATA_DIR + "/" + indexResource + "/";
  }

  /**
   * Delete a list of keys from s3, with a bulk delete request.
   *
   * @param s3Client s3 client
   * @param bucketName s3 bucket
   * @param keys keys to delete
   */
  public static void deleteObjects(AmazonS3 s3Client, String bucketName, List<String> keys) {
    System.out.println("Batch deleting objects, size: " + keys.size());
    DeleteObjectsRequest multiObjectDeleteRequest =
        new DeleteObjectsRequest(bucketName).withKeys(keys.toArray(new String[0])).withQuiet(true);
    s3Client.deleteObjects(multiObjectDeleteRequest);
  }

  /**
   * Parse an interval string into a numeric interval in ms. The string must be in a form 10s, 5h,
   * etc. Numeric component must be positive. The units component must be one of (s)econds,
   * (m)inutes, (h)ours, (d)ays.
   *
   * @param interval interval string
   * @return interval in ms
   */
  public static long getTimeIntervalMs(String interval) {
    String trimmed = interval.trim();
    if (trimmed.length() < 2) {
      throw new IllegalArgumentException("Invalid time interval: " + trimmed);
    }
    char endChar = trimmed.charAt(trimmed.length() - 1);
    long numberVal = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));

    if (numberVal < 1) {
      throw new IllegalArgumentException("Time interval must be > 0");
    }

    return switch (endChar) {
      case 's' -> TimeUnit.SECONDS.toMillis(numberVal);
      case 'm' -> TimeUnit.MINUTES.toMillis(numberVal);
      case 'h' -> TimeUnit.HOURS.toMillis(numberVal);
      case 'd' -> TimeUnit.DAYS.toMillis(numberVal);
      default -> throw new IllegalArgumentException("Unknown time unit: " + endChar);
    };
  }
}
