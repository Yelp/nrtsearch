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
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.utils.TimeStringUtil;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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
    String resource = S3Backend.getResourceName("exist_index", IndexResourceType.WARMING_QUERIES);
    String key = S3Backend.getVersionKey("exist_service", resource, S3Backend.LATEST_VERSION);
    s3.putObject(BUCKET_NAME, key, "1");

    assertTrue(s3Backend.exists("exist_service", "exist_index", IndexResourceType.WARMING_QUERIES));
    assertFalse(
        s3Backend.exists("exist_service_2", "exist_index", IndexResourceType.WARMING_QUERIES));
    assertFalse(
        s3Backend.exists("exist_service", "exist_index_2", IndexResourceType.WARMING_QUERIES));
  }

  @Test
  public void testExists_pointState() throws IOException {
    String keyPrefix = S3Backend.indexPointStateResourcePrefix("exist_service3", "exist_index");
    String key = keyPrefix + S3Backend.CURRENT_VERSION;
    s3.putObject(BUCKET_NAME, key, "version");

    assertTrue(s3Backend.exists("exist_service3", "exist_index", IndexResourceType.POINT_STATE));
    assertFalse(s3Backend.exists("exist_service_4", "exist_index", IndexResourceType.POINT_STATE));
    assertFalse(s3Backend.exists("exist_service3", "exist_index_2", IndexResourceType.POINT_STATE));
  }

  @Test
  public void testDownloadStream_singlePart() throws IOException {
    String resource =
        S3Backend.getResourceName("download_index", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("download_service", resource, S3Backend.LATEST_VERSION);
    s3.putObject(BUCKET_NAME, latestKey, "1");

    String versionKey = S3Backend.getVersionKey("download_service", resource, "1");
    String resourceHash = UUID.randomUUID().toString();
    s3.putObject(BUCKET_NAME, versionKey, resourceHash);

    String resourceKey = S3Backend.getResourceKey("download_service", resource, resourceHash);
    s3.putObject(BUCKET_NAME, resourceKey, "resource_data");

    InputStream downloadStream =
        s3Backend.downloadStream(
            "download_service", "download_index", IndexResourceType.WARMING_QUERIES);
    assertEquals("resource_data", convertToString(downloadStream));
  }

  @Test
  public void testDownloadStream_multiPart() throws IOException {
    String resource =
        S3Backend.getResourceName("download_index_2", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("download_service_2", resource, S3Backend.LATEST_VERSION);
    s3.putObject(BUCKET_NAME, latestKey, "1");

    String versionKey = S3Backend.getVersionKey("download_service_2", resource, "1");
    String resourceHash = UUID.randomUUID().toString();
    s3.putObject(BUCKET_NAME, versionKey, resourceHash);

    String resourceKey = S3Backend.getResourceKey("download_service_2", resource, resourceHash);
    putMultiPart(resourceKey, List.of("aaaa", "bbbb", "cccc", "dddd"));

    InputStream downloadStream =
        s3Backend.downloadStream(
            "download_service_2", "download_index_2", IndexResourceType.WARMING_QUERIES);
    assertEquals("aaaabbbbccccdddd", convertToString(downloadStream));
  }

  @Test
  public void testDownloadStream_notFound() throws IOException {
    try {
      s3Backend.downloadStream(
          "download_service_3", "download_index_3", IndexResourceType.WARMING_QUERIES);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_service_3/_version/download_index_3_warming_queries/_latest_version not found",
          e.getMessage());
    }

    String resource =
        S3Backend.getResourceName("download_index_3", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("download_service_3", resource, S3Backend.LATEST_VERSION);
    s3.putObject(BUCKET_NAME, latestKey, "1");

    try {
      s3Backend.downloadStream(
          "download_service_3", "download_index_3", IndexResourceType.WARMING_QUERIES);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_service_3/_version/download_index_3_warming_queries/1 not found",
          e.getMessage());
    }

    String versionKey = S3Backend.getVersionKey("download_service_3", resource, "1");
    String resourceHash = UUID.randomUUID().toString();
    s3.putObject(BUCKET_NAME, versionKey, resourceHash);

    try {
      s3Backend.downloadStream(
          "download_service_3", "download_index_3", IndexResourceType.WARMING_QUERIES);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Object s3://s3-backend-test/download_service_3/download_index_3_warming_queries/"
              + resourceHash
              + " not found",
          e.getMessage());
    }
  }

  @Test
  public void testDownloadStream_updateData() throws IOException {
    String resource =
        S3Backend.getResourceName("download_index_4", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("download_service_4", resource, S3Backend.LATEST_VERSION);
    s3.putObject(BUCKET_NAME, latestKey, "1");

    String versionKey = S3Backend.getVersionKey("download_service_4", resource, "1");
    String resourceHash = UUID.randomUUID().toString();
    s3.putObject(BUCKET_NAME, versionKey, resourceHash);

    String resourceKey = S3Backend.getResourceKey("download_service_4", resource, resourceHash);
    s3.putObject(BUCKET_NAME, resourceKey, "resource_data_1");

    InputStream downloadStream =
        s3Backend.downloadStream(
            "download_service_4", "download_index_4", IndexResourceType.WARMING_QUERIES);
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String resourceHash2 = UUID.randomUUID().toString();
    String resourceKey2 = S3Backend.getResourceKey("download_service_4", resource, resourceHash2);
    s3.putObject(BUCKET_NAME, resourceKey2, "resource_data_2");

    String versionKey2 = S3Backend.getVersionKey("download_service_4", resource, "2");
    s3.putObject(BUCKET_NAME, versionKey2, resourceHash2);
    s3.putObject(BUCKET_NAME, latestKey, "2");

    downloadStream =
        s3Backend.downloadStream(
            "download_service_4", "download_index_4", IndexResourceType.WARMING_QUERIES);
    assertEquals("resource_data_2", convertToString(downloadStream));
  }

  @Test
  public void testUploadFile() throws IOException {
    File uploadFile = folder.newFile("upload_file");
    Files.write(uploadFile.toPath(), "file_data".getBytes());

    s3Backend.uploadFile(
        "upload_service", "upload_index", IndexResourceType.WARMING_QUERIES, uploadFile.toPath());

    String resource = S3Backend.getResourceName("upload_index", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("upload_service", resource, S3Backend.LATEST_VERSION);
    String contents = convertToString(s3.getObject(BUCKET_NAME, latestKey).getObjectContent());
    assertEquals("0", contents);

    String versionKey = S3Backend.getVersionKey("upload_service", resource, "0");
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    // check parsable
    UUID.fromString(contents);

    String resourceKey = S3Backend.getResourceKey("upload_service", resource, contents);
    contents = convertToString(s3.getObject(BUCKET_NAME, resourceKey).getObjectContent());
    assertEquals("file_data", contents);
  }

  @Test
  public void testUploadFile_updateResource() throws IOException {
    File uploadFile = folder.newFile("upload_file_1");
    Files.write(uploadFile.toPath(), "file_data_1".getBytes());

    s3Backend.uploadFile(
        "upload_service_1",
        "upload_index_1",
        IndexResourceType.WARMING_QUERIES,
        uploadFile.toPath());

    String resource =
        S3Backend.getResourceName("upload_index_1", IndexResourceType.WARMING_QUERIES);
    String latestKey =
        S3Backend.getVersionKey("upload_service_1", resource, S3Backend.LATEST_VERSION);
    String contents = convertToString(s3.getObject(BUCKET_NAME, latestKey).getObjectContent());
    assertEquals("0", contents);

    String versionKey = S3Backend.getVersionKey("upload_service_1", resource, "0");
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    // check parsable
    UUID.fromString(contents);

    String resourceKey = S3Backend.getResourceKey("upload_service_1", resource, contents);
    contents = convertToString(s3.getObject(BUCKET_NAME, resourceKey).getObjectContent());
    assertEquals("file_data_1", contents);

    // update data
    uploadFile = folder.newFile("upload_file_2");
    Files.write(uploadFile.toPath(), "file_data_2".getBytes());

    s3Backend.uploadFile(
        "upload_service_1",
        "upload_index_1",
        IndexResourceType.WARMING_QUERIES,
        uploadFile.toPath());

    contents = convertToString(s3.getObject(BUCKET_NAME, latestKey).getObjectContent());
    assertEquals("1", contents);

    versionKey = S3Backend.getVersionKey("upload_service_1", resource, "1");
    contents = convertToString(s3.getObject(BUCKET_NAME, versionKey).getObjectContent());
    // check parsable
    UUID.fromString(contents);

    resourceKey = S3Backend.getResourceKey("upload_service_1", resource, contents);
    contents = convertToString(s3.getObject(BUCKET_NAME, resourceKey).getObjectContent());
    assertEquals("file_data_2", contents);
  }

  @Test
  public void testUploadFile_notExist() throws IOException {
    Path path = Path.of(folder.getRoot().toString(), "not_exist");
    try {
      s3Backend.uploadFile(
          "upload_service_2", "upload_index_2", IndexResourceType.WARMING_QUERIES, path);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("File does not exist:"));
      assertTrue(e.getMessage().contains("not_exist"));
    }
  }

  @Test
  public void testUploadFile_notRegularFile() throws IOException {
    Path path = folder.newFolder("upload_folder").toPath();
    try {
      s3Backend.uploadFile(
          "upload_service_3", "upload_index_3", IndexResourceType.WARMING_QUERIES, path);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("Is not regular file:"));
      assertTrue(e.getMessage().contains("upload_folder"));
    }
  }

  @Test
  public void testGetResourceName() {
    assertEquals(
        "index_1_warming_queries",
        S3Backend.getResourceName("index_1", IndexResourceType.WARMING_QUERIES));
  }

  @Test
  public void testGetResourceKey() {
    assertEquals(
        "test_service/test_resource/test_version_hash",
        S3Backend.getResourceKey("test_service", "test_resource", "test_version_hash"));
  }

  @Test
  public void testGetVersionKey() {
    assertEquals(
        "test_service/_version/test_resource/version",
        S3Backend.getVersionKey("test_service", "test_resource", "version"));
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

    String keyPrefix = S3Backend.indexDataResourcePrefix("upload_index_service", "upload_index");
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

    String keyPrefix =
        S3Backend.indexDataResourcePrefix("download_index_service", "download_index");
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

    String keyPrefix =
        S3Backend.indexDataResourcePrefix("download_index_service", "download_index");
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
    assertEquals("service/index/data/", S3Backend.indexDataResourcePrefix("service", "index"));
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

    s3Backend.uploadPointState("upload_point_service", "upload_point_index", pointState);

    String keyPrefix =
        S3Backend.indexPointStateResourcePrefix("upload_point_service", "upload_point_index");
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
        S3Backend.indexPointStateResourcePrefix("download_point_service", "download_point_index");
    String fileName = S3Backend.getPointStateFileName(pointState);
    s3.putObject(BUCKET_NAME, keyPrefix + fileName, getPointStateJson());
    s3.putObject(BUCKET_NAME, keyPrefix + S3Backend.CURRENT_VERSION, fileName);

    NrtPointState downloadedPointState =
        s3Backend.downloadPointState("download_point_service", "download_point_index");
    assertEquals(pointState, downloadedPointState);
  }

  @Test
  public void testIndexPointStateResourcePrefix() {
    assertEquals(
        "service/index/point_state/", S3Backend.indexPointStateResourcePrefix("service", "index"));
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
