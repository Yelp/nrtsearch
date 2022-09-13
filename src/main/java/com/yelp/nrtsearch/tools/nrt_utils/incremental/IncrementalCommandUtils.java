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
import com.amazonaws.services.s3.model.S3Object;
import com.yelp.nrtsearch.server.luceneserver.warming.Warmer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class IncrementalCommandUtils {
  private static final String INDEX_DATA_SUFFIX = "_data_index_data";
  public static final String SNAPSHOT_INDEX_STATE_FILE = "index_state";
  public static final String SNAPSHOT_INDEX_FILES = "index_files";
  public static final String SNAPSHOT_WARMING_QUERIES = "index_warming_queries";
  public static final String SNAPSHOT_DIR = "snapshots";
  public static final String METADATA_DIR = "metadata";

  private IncrementalCommandUtils() {}

  /**
   * Get the index data identifier given the base resource name.
   *
   * @param indexResource base index resource (name-UUID)
   * @return index data resource name
   */
  public static String getIndexDataResource(String indexResource) {
    return indexResource + INDEX_DATA_SUFFIX;
  }

  /**
   * Get the index warming queries identifier given the base resource name.
   *
   * @param indexResource base index resource (name-UUID)
   * @return index warming queries resource
   */
  public static String getWarmingQueriesResource(String indexResource) {
    return indexResource + Warmer.WARMING_QUERIES_RESOURCE;
  }

  /**
   * Get the s3 key prefix for index version files.
   *
   * @param serviceName nrtsearch cluster service name
   * @param indexDataResource index data resource name
   * @return key prefix
   */
  public static String getVersionKeyPrefix(String serviceName, String indexDataResource) {
    return String.format("%s/_version/%s/", serviceName, indexDataResource);
  }

  /**
   * Get the s3 key prefix for index data.
   *
   * @param serviceName nrtsearch cluster service name
   * @param indexDataResource index data resource name
   * @return key prefix
   */
  public static String getDataKeyPrefix(String serviceName, String indexDataResource) {
    return String.format("%s/%s/", serviceName, indexDataResource);
  }

  /**
   * Get the s3 key prefix for saved warming queries.
   *
   * @param serviceName nrtsearch cluster service name
   * @param warmingQueriesResource index warming queries resource name
   * @return key prefix
   */
  public static String getWarmingQueriesKeyPrefix(
      String serviceName, String warmingQueriesResource) {
    return String.format("%s/%s/", serviceName, warmingQueriesResource);
  }

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
   * Get the root key for index data for a specific snapshot timestamp.
   *
   * @param snapshotRoot root key for all snapshots
   * @param indexResource index resource (name-UUID)
   * @param timestampMs snapshot timestamp
   * @return root key for snapshot data
   */
  public static String getSnapshotIndexDataRoot(
      String snapshotRoot, String indexResource, long timestampMs) {
    return snapshotRoot + indexResource + "/" + timestampMs + "/";
  }

  /**
   * Get the S3 key for metadata object for a specific snapshot timestamp.
   *
   * @param snapshotRoot root key for all snapshots
   * @param indexResource index resource (name-UUID)
   * @param timestampMs snapshot timestamp
   * @return key for snapshot metadata
   */
  public static String getSnapshotIndexMetadataKey(
      String snapshotRoot, String indexResource, long timestampMs) {
    return snapshotRoot + METADATA_DIR + "/" + indexResource + "/" + timestampMs;
  }

  /**
   * Check if a file is a lucene index file.
   *
   * @param fileName name to check
   */
  public static boolean isDataFile(String fileName) {
    return fileName.startsWith("_") || fileName.startsWith("segments");
  }

  /**
   * Check if a file name is a valid manifest file (UUID).
   *
   * @param fileName name to check
   */
  public static boolean isManifestFile(String fileName) {
    return isUUID(fileName);
  }

  /**
   * Check if a string is a UUID.
   *
   * @param s string to check
   * @return if string is a UUID
   */
  public static boolean isUUID(String s) {
    try {
      UUID.fromString(s);
      return true;
    } catch (IllegalArgumentException ignore) {
      return false;
    }
  }

  /**
   * Get all index files that are part of the given index data version id (UUID).
   *
   * @param s3Client s3 client
   * @param bucketName s3 bucket
   * @param serviceName nrtsearch cluster service name
   * @param indexDataResource index data resource name
   * @param versionId data version UUID string
   * @return set of all index files for index version
   * @throws IOException
   */
  public static Set<String> getVersionFiles(
      AmazonS3 s3Client,
      String bucketName,
      String serviceName,
      String indexDataResource,
      String versionId)
      throws IOException {
    String versionPath = String.format("%s/%s/%s", serviceName, indexDataResource, versionId);
    S3Object s3Object = s3Client.getObject(bucketName, versionPath);

    String indexFileName;
    Set<String> indexFileNames = new HashSet<>();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
      while ((indexFileName = br.readLine()) != null) {
        indexFileNames.add(indexFileName);
      }
    }
    return indexFileNames;
  }
}
