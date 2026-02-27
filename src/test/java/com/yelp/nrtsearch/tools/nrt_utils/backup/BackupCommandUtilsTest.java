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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.IOException;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class BackupCommandUtilsTest {
  private static final String TEST_BUCKET = "test-backup-bucket";

  @Rule public AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  @Test
  public void testGetSnapshotRoot_rootProvided() {
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root/", null));
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root/", "service"));
    assertEquals("root/", BackupCommandUtils.getSnapshotRoot("root", "service"));
  }

  @Test
  public void testGetSnapshotRoot_serviceProvided() {
    assertEquals(
        "service_name/snapshots/", BackupCommandUtils.getSnapshotRoot(null, "service_name"));
  }

  @Test
  public void testGetSnapshotRoot_neitherProvided() {
    try {
      BackupCommandUtils.getSnapshotRoot(null, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Must specify snapshotRoot or serviceName", e.getMessage());
    }
  }

  @Test
  public void testGetSnapshotIndexDataRoot() {
    assertEquals(
        "root/test_index/time_string/",
        BackupCommandUtils.getSnapshotIndexDataRoot("root/", "test_index", "time_string"));
  }

  @Test
  public void testGetSnapshotIndexMetadataKey() {
    assertEquals(
        "root/metadata/test_index/time_string",
        BackupCommandUtils.getSnapshotIndexMetadataKey("root/", "test_index", "time_string"));
  }

  @Test
  public void testGetSnapshotIndexMetadataPrefix() {
    assertEquals(
        "root/metadata/test_index/",
        BackupCommandUtils.getSnapshotIndexMetadataPrefix("root/", "test_index"));
  }

  @Test
  public void testGetTimeIntervalMs() {
    assertEquals(1000, BackupCommandUtils.getTimeIntervalMs("1s"));
    assertEquals(60000, BackupCommandUtils.getTimeIntervalMs("1m"));
    assertEquals(3600000, BackupCommandUtils.getTimeIntervalMs("1h"));
    assertEquals(86400000, BackupCommandUtils.getTimeIntervalMs("1d"));
  }

  @Test
  public void testGetTimeIntervalMs_invalid() {
    try {
      BackupCommandUtils.getTimeIntervalMs("1x");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unknown time unit: x", e.getMessage());
    }
  }

  @Test
  public void testExtractDirectoryPrefixes_emptyBucket() {
    S3Client s3Client = s3Provider.getS3Client();
    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, "snapshots/");
    assertTrue("Empty bucket should return empty set", prefixes.isEmpty());
  }

  @Test
  public void testExtractDirectoryPrefixes_singleDirectory() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create object: snapshots/index1/20230101120000000/file.txt
    putObject(s3Client, prefix + "20230101120000000/file.txt", "content");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should find one directory prefix", 1, prefixes.size());
    assertTrue(
        "Should contain timestamp directory", prefixes.contains(prefix + "20230101120000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_multipleDirectories() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create objects in different timestamp directories
    putObject(s3Client, prefix + "20230101120000000/file1.txt", "content1");
    putObject(s3Client, prefix + "20230101130000000/file2.txt", "content2");
    putObject(s3Client, prefix + "20230101140000000/file3.txt", "content3");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should find three directory prefixes", 3, prefixes.size());
    assertTrue(prefixes.contains(prefix + "20230101120000000/"));
    assertTrue(prefixes.contains(prefix + "20230101130000000/"));
    assertTrue(prefixes.contains(prefix + "20230101140000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_multipleFilesInSameDirectory() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create multiple files in same timestamp directory
    putObject(s3Client, prefix + "20230101120000000/file1.txt", "content1");
    putObject(s3Client, prefix + "20230101120000000/file2.txt", "content2");
    putObject(s3Client, prefix + "20230101120000000/subdir/file3.txt", "content3");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should find one directory prefix despite multiple files", 1, prefixes.size());
    assertTrue(prefixes.contains(prefix + "20230101120000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_nestedDirectories() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create nested structure
    putObject(s3Client, prefix + "20230101120000000/subdir1/file1.txt", "content1");
    putObject(s3Client, prefix + "20230101120000000/subdir2/file2.txt", "content2");
    putObject(s3Client, prefix + "20230101130000000/file3.txt", "content3");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should only extract first-level directories", 2, prefixes.size());
    assertTrue(prefixes.contains(prefix + "20230101120000000/"));
    assertTrue(prefixes.contains(prefix + "20230101130000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_fileAtPrefixLevel() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create file directly at prefix level (not in subdirectory)
    putObject(s3Client, prefix + "README.txt", "readme content");
    putObject(s3Client, prefix + "20230101120000000/file.txt", "content");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should only include directory prefix, not direct files", 1, prefixes.size());
    assertTrue(prefixes.contains(prefix + "20230101120000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_differentPrefixes() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();

    // Create objects under different base prefixes
    putObject(s3Client, "snapshots/index1/20230101120000000/file1.txt", "content1");
    putObject(s3Client, "snapshots/index2/20230101130000000/file2.txt", "content2");

    // Query for index1
    Set<String> prefixes1 =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, "snapshots/index1/");
    assertEquals("Should find only index1 directory", 1, prefixes1.size());
    assertTrue(prefixes1.contains("snapshots/index1/20230101120000000/"));

    // Query for index2
    Set<String> prefixes2 =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, "snapshots/index2/");
    assertEquals("Should find only index2 directory", 1, prefixes2.size());
    assertTrue(prefixes2.contains("snapshots/index2/20230101130000000/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_specialCharacters() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/test-index_v2/";

    // Create objects with special characters in directory names
    putObject(s3Client, prefix + "2023-01-01_12-00-00/file.txt", "content");

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should handle special characters in directory names", 1, prefixes.size());
    assertTrue(prefixes.contains(prefix + "2023-01-01_12-00-00/"));
  }

  @Test
  public void testExtractDirectoryPrefixes_manyDirectories() throws IOException {
    S3Client s3Client = s3Provider.getS3Client();
    String prefix = "snapshots/index1/";

    // Create 10 directories to test pagination handling
    for (int i = 0; i < 10; i++) {
      String timestamp = String.format("2023010112%02d00000", i);
      putObject(s3Client, prefix + timestamp + "/file.txt", "content" + i);
    }

    Set<String> prefixes =
        BackupCommandUtils.extractDirectoryPrefixes(s3Client, TEST_BUCKET, prefix);
    assertEquals("Should find all 10 directory prefixes", 10, prefixes.size());
    for (int i = 0; i < 10; i++) {
      String timestamp = String.format("2023010112%02d00000", i);
      assertTrue(
          "Should contain directory " + timestamp, prefixes.contains(prefix + timestamp + "/"));
    }
  }

  private void putObject(S3Client s3Client, String key, String content) {
    PutObjectRequest request = PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build();
    s3Client.putObject(request, RequestBody.fromString(content));
  }
}
