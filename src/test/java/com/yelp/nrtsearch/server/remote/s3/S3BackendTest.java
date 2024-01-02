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
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
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
import java.util.UUID;
import org.apache.commons.io.IOUtils;
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
