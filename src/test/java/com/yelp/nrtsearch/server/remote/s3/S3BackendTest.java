/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.remote.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.utils.TimeStringUtil;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class S3BackendTest {
  private static final String BUCKET_NAME = "s3-backend-test";
  private static final String KEY = "test_key";
  private static final String CONTENT = "test_content";

  @ClassRule public static final AmazonS3Provider S3_PROVIDER = new AmazonS3Provider(BUCKET_NAME);

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static AmazonS3 s3;
  private static S3Backend s3Backend;

  @BeforeClass
  public static void setup() throws IOException {
    String configStr = "bucketName: " + BUCKET_NAME;
    LuceneServerConfiguration config =
        new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
    s3 = S3_PROVIDER.getAmazonS3();
    s3Backend = new S3Backend(config, s3);
    s3.putObject(BUCKET_NAME, KEY, CONTENT);
  }

  @AfterClass
  public static void cleanUp() {
    s3Backend.close();
  }

  @Test
  public void testGetS3() {
    assertEquals(s3, s3Backend.getS3());
  }

  @Test
  public void testDownloadFromS3Path() throws IOException {
    InputStream inputStream = s3Backend.downloadFromS3Path(BUCKET_NAME, KEY);
    String contentFromS3 = convertToString(inputStream);
    assertEquals(CONTENT, contentFromS3);

    inputStream = s3Backend.downloadFromS3Path(String.format("s3://%s/%s", BUCKET_NAME, KEY));
    contentFromS3 = convertToString(inputStream);
    assertEquals(CONTENT, contentFromS3);
  }

  @Test
  public void testDownloadFromS3Path_fileDoesNotExist() {
    String s3Path = String.format("s3://%s/%s", BUCKET_NAME, "does_not_exist");
    try {
      s3Backend.downloadFromS3Path(s3Path);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(String.format("Object %s not found", s3Path), e.getMessage());
    }

    try {
      s3Backend.downloadFromS3Path("bucket_not_exist", KEY);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          String.format("Object s3://%s/%s not found", "bucket_not_exist", KEY), e.getMessage());
    }
  }

  @Test
  public void testExists() throws IOException {
    exists(IndexResourceType.WARMING_QUERIES, S3Backend.WARMING, IndexResourceType.WARMING_QUERIES);
    exists(IndexResourceType.POINT_STATE, S3Backend.POINT_STATE, IndexResourceType.POINT_STATE);
    exists(IndexResourceType.INDEX_STATE, S3Backend.INDEX_STATE, IndexResourceType.INDEX_STATE);
  }

  private void exists(
      IndexResourceType indexResourceType, String resourceName, IndexResourceType resourceType)
      throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "exist_service" + resourceName, "exist_index", resourceType);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);

    assertTrue(s3Backend.exists("exist_service" + resourceName, "exist_index", indexResourceType));
    assertFalse(
        s3Backend.exists("exist_service_2" + resourceName, "exist_index", indexResourceType));
    assertFalse(
        s3Backend.exists("exist_service" + resourceName, "exist_index_2", indexResourceType));
  }

  @Test
  public void testExists_globalState() throws IOException {
    String prefix = S3Backend.getGlobalStateResourcePrefix("global_exist_service");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);

    assertTrue(
        s3Backend.exists("global_exist_service", RemoteBackend.GlobalResourceType.GLOBAL_STATE));
    assertFalse(
        s3Backend.exists("global_exist_service_2", RemoteBackend.GlobalResourceType.GLOBAL_STATE));
  }

  @Test
  public void testDownloadGlobalState_singlePart() throws IOException {
    String prefix = S3Backend.getGlobalStateResourcePrefix("download_global_service");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data");

    InputStream downloadStream = s3Backend.downloadGlobalState("download_global_service");
    assertEquals("resource_data", convertToString(downloadStream));
  }

  @Test
  public void testDownloadGlobalState_multiPart() throws IOException {
    String prefix = S3Backend.getGlobalStateResourcePrefix("download_global_service_2");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    putMultiPart(prefix + fileName, List.of("aaaa", "bbbb", "cccc", "dddd"));

    InputStream downloadStream = s3Backend.downloadGlobalState("download_global_service_2");
    assertEquals("aaaabbbbccccdddd", convertToString(downloadStream));
  }

  @Test
  public void testDownloadGlobalState_notFound() throws IOException {
    try {
      s3Backend.downloadGlobalState("download_global_service_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_global_service_3/global_state/_current not found",
          e.getMessage());
    }

    String prefix = S3Backend.getGlobalStateResourcePrefix("download_global_service_3");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);

    try {
      s3Backend.downloadGlobalState("download_global_service_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_global_service_3/global_state/"
              + fileName
              + " not found",
          e.getMessage());
    }
  }

  @Test
  public void testDownloadGlobalState_updateData() throws IOException {
    String prefix = S3Backend.getGlobalStateResourcePrefix("download_global_service_4");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data_1");

    InputStream downloadStream = s3Backend.downloadGlobalState("download_global_service_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getGlobalStateFileName();
    s3.putObject(BUCKET_NAME, prefix + fileName2, "resource_data_2");
    s3.putObject(BUCKET_NAME, currentKey, fileName2);

    downloadStream = s3Backend.downloadGlobalState("download_global_service_4");
    assertEquals("resource_data_2", convertToString(downloadStream));
  }

  @Test
  public void testUploadGlobalState() throws IOException {
    byte[] data = "file_data".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadGlobalState("upload_global_service", data);

    String prefix = S3Backend.getGlobalStateResourcePrefix("upload_global_service");
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertGlobalStateVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data", contents);
  }

  @Test
  public void testUploadGlobalState_updateResource() throws IOException {
    byte[] data = "file_data_1".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadGlobalState("upload_global_service_1", data);

    String prefix = S3Backend.getGlobalStateResourcePrefix("upload_global_service_1");
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertGlobalStateVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadGlobalState("upload_global_service_1", data);

    contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertGlobalStateVersion(contents);

    versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_2", contents);
  }

  @Test
  public void testGetGlobalStateResourcePrefix() {
    assertEquals("service/global_state/", S3Backend.getGlobalStateResourcePrefix("service"));
  }

  @Test
  public void testGetGlobalStateFileName() {
    String fileName = S3Backend.getGlobalStateFileName();
    assertGlobalStateVersion(fileName);
  }

  private void assertGlobalStateVersion(String globalStateVersion) {
    String[] splits = globalStateVersion.split("-", 2);
    assertEquals(2, splits.length);
    // check parsable
    assertTrue(TimeStringUtil.isTimeStringSec(splits[0]));
    UUID.fromString(splits[1]);
  }

  @Test
  public void testDownloadIndexState_singlePart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_index_state_service", "download_index", IndexResourceType.INDEX_STATE);
    String fileName = S3Backend.getIndexStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data");

    InputStream downloadStream =
        s3Backend.downloadIndexState("download_index_state_service", "download_index");
    assertEquals("resource_data", convertToString(downloadStream));
  }

  @Test
  public void testDownloadIndexState_multiPart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_index_state_service_2", "download_index_2", IndexResourceType.INDEX_STATE);
    String fileName = S3Backend.getIndexStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    putMultiPart(prefix + fileName, List.of("aaaa", "bbbb", "cccc", "dddd"));

    InputStream downloadStream =
        s3Backend.downloadIndexState("download_index_state_service_2", "download_index_2");
    assertEquals("aaaabbbbccccdddd", convertToString(downloadStream));
  }

  @Test
  public void testDownloadIndexState_notFound() throws IOException {
    try {
      s3Backend.downloadIndexState("download_index_state_service_3", "download_index_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_index_state_service_3/download_index_3/state/_current not found",
          e.getMessage());
    }

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_index_state_service_3", "download_index_3", IndexResourceType.INDEX_STATE);
    String fileName = S3Backend.getIndexStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);

    try {
      s3Backend.downloadIndexState("download_index_state_service_3", "download_index_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_index_state_service_3/download_index_3/state/"
              + fileName
              + " not found",
          e.getMessage());
    }
  }

  @Test
  public void testDownloadIndexState_updateData() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_index_state_service_4", "download_index_4", IndexResourceType.INDEX_STATE);
    String fileName = S3Backend.getIndexStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data_1");

    InputStream downloadStream =
        s3Backend.downloadIndexState("download_index_state_service_4", "download_index_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getIndexStateFileName();
    s3.putObject(BUCKET_NAME, prefix + fileName2, "resource_data_2");
    s3.putObject(BUCKET_NAME, currentKey, fileName2);

    downloadStream =
        s3Backend.downloadIndexState("download_index_state_service_4", "download_index_4");
    assertEquals("resource_data_2", convertToString(downloadStream));
  }

  @Test
  public void testUploadIndexState() throws IOException {
    byte[] data = "file_data".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadIndexState("upload_index_state_service", "upload_index", data);

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "upload_index_state_service", "upload_index", IndexResourceType.INDEX_STATE);
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertIndexStateVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data", contents);
  }

  @Test
  public void testUploadIndexState_updateResource() throws IOException {
    byte[] data = "file_data_1".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadIndexState("upload_index_state_service_1", "upload_index_1", data);

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "upload_index_state_service_1", "upload_index_1", IndexResourceType.INDEX_STATE);
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertIndexStateVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadIndexState("upload_index_state_service_1", "upload_index_1", data);

    contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertIndexStateVersion(contents);

    versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_2", contents);
  }

  @Test
  public void testIndexStateResourcePrefix() {
    assertEquals(
        "service/index/state/",
        S3Backend.getIndexResourcePrefix("service", "index", IndexResourceType.INDEX_STATE));
  }

  @Test
  public void testGetIndexStateFileName() {
    String fileName = S3Backend.getIndexStateFileName();
    assertIndexStateVersion(fileName);
  }

  private void assertIndexStateVersion(String indexStateVersion) {
    String[] splits = indexStateVersion.split("-", 2);
    assertEquals(2, splits.length);
    // check parsable
    assertTrue(TimeStringUtil.isTimeStringSec(splits[0]));
    UUID.fromString(splits[1]);
  }

  @Test
  public void testDownloadWarmingQueries_singlePart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_warming_service", "download_index", IndexResourceType.WARMING_QUERIES);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data");

    InputStream downloadStream =
        s3Backend.downloadWarmingQueries("download_warming_service", "download_index");
    assertEquals("resource_data", convertToString(downloadStream));
  }

  @Test
  public void testDownloadWarmingQueries_multiPart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_warming_service_2", "download_index_2", IndexResourceType.WARMING_QUERIES);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    putMultiPart(prefix + fileName, List.of("aaaa", "bbbb", "cccc", "dddd"));

    InputStream downloadStream =
        s3Backend.downloadWarmingQueries("download_warming_service_2", "download_index_2");
    assertEquals("aaaabbbbccccdddd", convertToString(downloadStream));
  }

  @Test
  public void testDownloadWarmingQueries_notFound() throws IOException {
    try {
      s3Backend.downloadWarmingQueries("download_warming_service_3", "download_index_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_warming_service_3/download_index_3/warming/_current not found",
          e.getMessage());
    }

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_warming_service_3", "download_index_3", IndexResourceType.WARMING_QUERIES);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);

    try {
      s3Backend.downloadWarmingQueries("download_warming_service_3", "download_index_3");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_warming_service_3/download_index_3/warming/"
              + fileName
              + " not found",
          e.getMessage());
    }
  }

  @Test
  public void testDownloadWarmingQueries_updateData() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_warming_service_4", "download_index_4", IndexResourceType.WARMING_QUERIES);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, currentKey, fileName);
    s3.putObject(BUCKET_NAME, prefix + fileName, "resource_data_1");

    InputStream downloadStream =
        s3Backend.downloadWarmingQueries("download_warming_service_4", "download_index_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getWarmingQueriesFileName();
    s3.putObject(BUCKET_NAME, prefix + fileName2, "resource_data_2");
    s3.putObject(BUCKET_NAME, currentKey, fileName2);

    downloadStream =
        s3Backend.downloadWarmingQueries("download_warming_service_4", "download_index_4");
    assertEquals("resource_data_2", convertToString(downloadStream));
  }

  @Test
  public void testUploadWarmingQueries() throws IOException {
    byte[] data = "file_data".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadWarmingQueries("upload_warming_service", "upload_index", data);

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "upload_warming_service", "upload_index", IndexResourceType.WARMING_QUERIES);
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertWarmingVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data", contents);
  }

  @Test
  public void testUploadWarmingQueries_updateResource() throws IOException {
    byte[] data = "file_data_1".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadWarmingQueries("upload_warming_service_1", "upload_index_1", data);

    String prefix =
        S3Backend.getIndexResourcePrefix(
            "upload_warming_service_1", "upload_index_1", IndexResourceType.WARMING_QUERIES);
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertWarmingVersion(contents);

    String versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadWarmingQueries("upload_warming_service_1", "upload_index_1", data);

    contents = convertToString(s3.getObject(BUCKET_NAME, currentKey).getObjectContent());
    assertWarmingVersion(contents);

    versionKey = prefix + contents;
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    assertEquals("file_data_2", contents);
  }

  @Test
  public void testWarmingQueriesResourcePrefix() {
    assertEquals(
        "service/index/warming/",
        S3Backend.getIndexResourcePrefix("service", "index", IndexResourceType.WARMING_QUERIES));
  }

  @Test
  public void testGetWarmingQueriesFileName() {
    String fileName = S3Backend.getWarmingQueriesFileName();
    assertWarmingVersion(fileName);
  }

  private void assertWarmingVersion(String warmingVersion) {
    String[] splits = warmingVersion.split("-", 2);
    assertEquals(2, splits.length);
    // check parsable
    assertTrue(TimeStringUtil.isTimeStringSec(splits[0]));
    UUID.fromString(splits[1]);
  }

  @Test
  public void testUploadIndexFiles() throws IOException {
    File indexDir = folder.newFolder("index_dir");
    File file1 = new File(indexDir, "file1");
    Files.write(file1.toPath(), "file1_data".getBytes());
    File file2 = new File(indexDir, "file2");
    Files.write(file2.toPath(), "file2_data".getBytes());

    NrtFileMetaData fileMetaData1 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid1", "time_string_1");
    NrtFileMetaData fileMetaData2 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid2", "time_string_2");

    s3Backend.uploadIndexFiles(
        "upload_index_service",
        "upload_index",
        indexDir.toPath(),
        Map.of("file1", fileMetaData1, "file2", fileMetaData2));

    String keyPrefix = S3Backend.getIndexDataPrefix("upload_index_service", "upload_index");
    String filename1 = S3Backend.getIndexBackendFileName("file1", fileMetaData1);
    String filename2 = S3Backend.getIndexBackendFileName("file2", fileMetaData2);

    String contents =
        convertToString(s3.getObject(BUCKET_NAME, keyPrefix + filename1).getObjectContent());
    assertEquals("file1_data", contents);
    contents = convertToString(s3.getObject(BUCKET_NAME, keyPrefix + filename2).getObjectContent());
    assertEquals("file2_data", contents);
  }

  @Test
  public void testUploadIndexFiles_notFound() throws IOException {
    File indexDir = folder.newFolder("index_dir");
    File file1 = new File(indexDir, "file1");
    Files.write(file1.toPath(), "file1_data".getBytes());
    File file2 = new File(indexDir, "file2");
    Files.write(file2.toPath(), "file2_data".getBytes());

    NrtFileMetaData fileMetaData1 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid1", "time_string_1");
    NrtFileMetaData fileMetaData2 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid2", "time_string_2");
    NrtFileMetaData fileMetaDataNotExist =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid3", "time_string_3");

    try {
      s3Backend.uploadIndexFiles(
          "upload_index_service_2",
          "upload_index",
          indexDir.toPath(),
          Map.of(
              "file1", fileMetaData1, "not_exist", fileMetaDataNotExist, "file2", fileMetaData2));
      fail();
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("No such file or directory"));
    }
  }

  @Test
  public void testDownloadIndexFiles() throws IOException {
    File indexDir = folder.newFolder("index_dir");

    NrtFileMetaData fileMetaData1 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid1", "time_string_1");
    NrtFileMetaData fileMetaData2 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid2", "time_string_2");

    String keyPrefix = S3Backend.getIndexDataPrefix("download_index_service", "download_index");
    String filename1 = S3Backend.getIndexBackendFileName("file1", fileMetaData1);
    String filename2 = S3Backend.getIndexBackendFileName("file2", fileMetaData2);

    s3.putObject(BUCKET_NAME, keyPrefix + filename1, "file1_data");
    s3.putObject(BUCKET_NAME, keyPrefix + filename2, "file2_data");

    s3Backend.downloadIndexFiles(
        "download_index_service",
        "download_index",
        indexDir.toPath(),
        Map.of("file1", fileMetaData1, "file2", fileMetaData2));

    String contents = convertToString(Files.newInputStream(indexDir.toPath().resolve("file1")));
    assertEquals("file1_data", contents);
    contents = convertToString(Files.newInputStream(indexDir.toPath().resolve("file2")));
    assertEquals("file2_data", contents);
  }

  @Test
  public void testDownloadIndexFiles_notFound() throws IOException {
    File indexDir = folder.newFolder("index_dir");

    NrtFileMetaData fileMetaData1 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid1", "time_string_1");
    NrtFileMetaData fileMetaData2 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid2", "time_string_2");
    NrtFileMetaData fileMetaDataNotExist =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid3", "time_string_3");

    String keyPrefix = S3Backend.getIndexDataPrefix("download_index_service", "download_index");
    String filename1 = S3Backend.getIndexBackendFileName("file1", fileMetaData1);
    String filename2 = S3Backend.getIndexBackendFileName("file2", fileMetaData2);

    s3.putObject(BUCKET_NAME, keyPrefix + filename1, "file1_data");
    s3.putObject(BUCKET_NAME, keyPrefix + filename2, "file2_data");

    try {
      s3Backend.downloadIndexFiles(
          "download_index_service",
          "download_index",
          indexDir.toPath(),
          Map.of(
              "file1", fileMetaData1, "not_exist", fileMetaDataNotExist, "file2", fileMetaData2));
      fail();
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("Not Found"));
    }
  }

  @Test
  public void testGetIndexBackendFileName() {
    NrtFileMetaData fileMetaData =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid", "time_string");
    assertEquals(
        "time_string-pid-file_name", S3Backend.getIndexBackendFileName("file_name", fileMetaData));
  }

  @Test
  public void testIndexDataResourcePrefix() {
    assertEquals("service/index/data/", S3Backend.getIndexDataPrefix("service", "index"));
  }

  @Test
  public void testGetFileNamePairs() throws IOException {
    NrtFileMetaData fileMetaData1 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid1", "time_string_1");
    NrtFileMetaData fileMetaData2 =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "pid2", "time_string_2");

    List<S3Backend.FileNamePair> fileNamePairs =
        S3Backend.getFileNamePairs(Map.of("file1", fileMetaData1, "file2", fileMetaData2));
    S3Backend.FileNamePair expected1 =
        new S3Backend.FileNamePair("file1", "time_string_1-pid1-file1");
    S3Backend.FileNamePair expected2 =
        new S3Backend.FileNamePair("file2", "time_string_2-pid2-file2");
    assertEquals(2, fileNamePairs.size());
    assertTrue(fileNamePairs.contains(expected1));
    assertTrue(fileNamePairs.contains(expected2));
  }

  @Test
  public void testUploadPointState() throws IOException {
    NrtPointState pointState = getPointState();
    byte[] pointStateBytes = RemoteUtils.pointStateToUtf8(pointState);

    s3Backend.uploadPointState(
        "upload_point_service", "upload_point_index", pointState, pointStateBytes);

    String keyPrefix =
        S3Backend.getIndexResourcePrefix(
            "upload_point_service", "upload_point_index", IndexResourceType.POINT_STATE);
    String fileName = S3Backend.getPointStateFileName(pointState);
    String contents =
        convertToString(s3.getObject(BUCKET_NAME, keyPrefix + fileName).getObjectContent());
    assertEquals(getPointStateJson(), contents);

    String latestKey = keyPrefix + S3Backend.CURRENT_VERSION;
    contents = convertToString(s3.getObject(BUCKET_NAME, latestKey).getObjectContent());
    assertEquals(fileName, contents);
  }

  @Test
  public void testDownloadPointState() throws IOException {
    NrtPointState pointState = getPointState();
    String keyPrefix =
        S3Backend.getIndexResourcePrefix(
            "download_point_service", "download_point_index", IndexResourceType.POINT_STATE);
    String fileName = S3Backend.getPointStateFileName(pointState);
    s3.putObject(BUCKET_NAME, keyPrefix + fileName, getPointStateJson());
    s3.putObject(BUCKET_NAME, keyPrefix + S3Backend.CURRENT_VERSION, fileName);

    byte[] downloadedData =
        s3Backend
            .downloadPointState("download_point_service", "download_point_index")
            .readAllBytes();
    NrtPointState downloadedPointState = RemoteUtils.pointStateFromUtf8(downloadedData);
    assertEquals(pointState, downloadedPointState);
  }

  @Test
  public void testIndexPointStateResourcePrefix() {
    assertEquals(
        "service/index/point_state/",
        S3Backend.getIndexResourcePrefix("service", "index", IndexResourceType.POINT_STATE));
  }

  @Test
  public void testGetPointStateFileName() {
    NrtPointState pointState = getPointState();
    String fileName = S3Backend.getPointStateFileName(pointState);
    String[] components = fileName.split("-");
    assertEquals(3, components.length);
    assertTrue(TimeStringUtil.isTimeStringSec(components[0]));
    assertEquals("primaryId", components[1]);
    assertEquals("1", components[2]);
  }

  private NrtPointState getPointState() {
    long version = 1;
    long gen = 3;
    byte[] infosBytes = new byte[] {1, 2, 3, 4, 5};
    long primaryGen = 5;
    Set<String> completedMergeFiles = Set.of("file1");
    String primaryId = "primaryId";
    FileMetaData fileMetaData =
        new FileMetaData(new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25);
    NrtFileMetaData nrtFileMetaData =
        new NrtFileMetaData(
            new byte[] {6, 7, 8}, new byte[] {0, 10, 11}, 10, 25, "primaryId2", "timeString");
    CopyState copyState =
        new CopyState(
            Map.of("file3", fileMetaData),
            version,
            gen,
            infosBytes,
            completedMergeFiles,
            primaryGen,
            null);
    return new NrtPointState(copyState, Map.of("file3", nrtFileMetaData), primaryId);
  }

  private String getPointStateJson() {
    return "{\"files\":{\"file3\":{\"header\":\"BgcI\",\"footer\":\"AAoL\",\"length\":10,\"checksum\":25,\"primaryId\":\"primaryId2\",\"timeString\":\"timeString\"}},\"version\":1,\"gen\":3,\"infosBytes\":\"AQIDBAU=\",\"primaryGen\":5,\"completedMergeFiles\":[\"file1\"],\"primaryId\":\"primaryId\"}";
  }

  @Test
  public void testGetCurrentResourceName() throws IOException {
    String prefix = "service_get_current/resource/";
    s3.putObject(BUCKET_NAME, prefix + S3Backend.CURRENT_VERSION, "current_version_name");
    assertEquals("current_version_name", s3Backend.getCurrentResourceName(prefix));
  }

  @Test
  public void testGetCurrentResourceName_notFound() throws IOException {
    String prefix = "service_get_current_not_found/resource/";
    try {
      s3Backend.getCurrentResourceName(prefix);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/service_get_current_not_found/resource/_current not found",
          e.getMessage());
    }
  }

  @Test
  public void testSetCurrentResource() throws IOException {
    String prefix = "service_set_current/resource/";
    s3Backend.setCurrentResource(prefix, "current_version_name");
    String contents =
        convertToString(
            s3.getObject(BUCKET_NAME, prefix + S3Backend.CURRENT_VERSION).getObjectContent());
    assertEquals("current_version_name", contents);
  }

  @Test
  public void testCurrentResourceExists() {
    String prefix = "service_current_exists/resource/";
    assertFalse(s3Backend.currentResourceExists(prefix));
    s3.putObject(BUCKET_NAME, prefix + S3Backend.CURRENT_VERSION, "current_version_name");
    assertTrue(s3Backend.currentResourceExists(prefix));
  }

  private void putMultiPart(String key, List<String> partsData) {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(BUCKET_NAME, key);
    InitiateMultipartUploadResult initResponse = s3.initiateMultipartUpload(initRequest);

    List<PartETag> partETags = new ArrayList<>();
    for (int i = 0; i < partsData.size(); ++i) {
      UploadPartRequest uploadRequest =
          new UploadPartRequest()
              .withBucketName(BUCKET_NAME)
              .withKey(key)
              .withUploadId(initResponse.getUploadId())
              .withPartNumber(i + 1)
              .withInputStream(new ByteArrayInputStream(partsData.get(i).getBytes()))
              .withPartSize(partsData.get(i).length());

      UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
      partETags.add(uploadResult.getPartETag());
    }

    CompleteMultipartUploadRequest compRequest =
        new CompleteMultipartUploadRequest(BUCKET_NAME, key, initResponse.getUploadId(), partETags);
    s3.completeMultipartUpload(compRequest);
  }

  private String convertToString(InputStream inputStream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer);
    return writer.toString();
  }
}
