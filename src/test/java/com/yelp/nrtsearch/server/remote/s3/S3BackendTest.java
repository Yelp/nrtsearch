/*
 * Copyright 2025 Yelp Inc.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.monitoring.S3DownloadStreamWrapper;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.utils.GlobalThrottledInputStream;
import com.yelp.nrtsearch.server.utils.GlobalWindowRateLimiter;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3BackendTest {
  private static final String BUCKET_NAME = "s3-backend-test";
  private static final String KEY = "test_key";
  private static final String CONTENT = "test_content";

  @ClassRule public static final AmazonS3Provider S3_PROVIDER = new AmazonS3Provider(BUCKET_NAME);

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static S3Client s3;
  private static S3Backend s3Backend;
  private static ExecutorFactory executorFactory;

  @BeforeClass
  public static void setup() throws IOException {
    String configStr = "bucketName: " + BUCKET_NAME;
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    executorFactory = new ExecutorFactory(config.getThreadPoolConfiguration());
    s3 = S3_PROVIDER.getS3Client();
    s3Backend = new S3Backend(config, s3, executorFactory);
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(KEY)
            .build(),
        RequestBody.fromString(CONTENT));
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
    InputStream inputStream = s3Backend.downloadFromS3Path(BUCKET_NAME, KEY, true);
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
      s3Backend.downloadFromS3Path("bucket_not_exist", KEY, true);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          String.format("Object s3://%s/%s not found", "bucket_not_exist", KEY), e.getMessage());
    }
  }

  @Test
  public void testUsesRemoteExecutor() {
    assertSame(
        executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE), s3Backend.getExecutor());
  }

  @Test
  public void testDefaultExecutor() {
    try (S3Backend s3Backend =
        new S3Backend(BUCKET_NAME, false, S3Backend.DEFAULT_CONFIG, mock(S3Client.class))) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) s3Backend.getExecutor();
      assertNotSame(executorFactory.getExecutor(ExecutorFactory.ExecutorType.REMOTE), executor);
      assertEquals(ThreadPoolConfiguration.DEFAULT_REMOTE_THREADS, executor.getCorePoolSize());
      assertEquals(
          ThreadPoolConfiguration.DEFAULT_REMOTE_BUFFERED_ITEMS,
          executor.getQueue().remainingCapacity());
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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(prefix + fileName)
            .build(),
        RequestBody.fromString("resource_data"));

    InputStream downloadStream = s3Backend.downloadGlobalState("download_global_service");
    assertEquals("resource_data", convertToString(downloadStream));
  }

  @Test
  public void testDownloadGlobalState_multiPart() throws IOException {
    String prefix = S3Backend.getGlobalStateResourcePrefix("download_global_service_2");
    String fileName = S3Backend.getGlobalStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName).build(),
        RequestBody.fromString("resource_data_1"));

    InputStream downloadStream = s3Backend.downloadGlobalState("download_global_service_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getGlobalStateFileName();
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName2).build(),
        RequestBody.fromString("resource_data_2"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
        RequestBody.fromString(fileName2));

    downloadStream = s3Backend.downloadGlobalState("download_global_service_4");
    assertEquals("resource_data_2", convertToString(downloadStream));
  }

  @Test
  public void testUploadGlobalState() throws IOException {
    byte[] data = "file_data".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadGlobalState("upload_global_service", data);

    String prefix = S3Backend.getGlobalStateResourcePrefix("upload_global_service");
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertGlobalStateVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
    assertEquals("file_data", contents);
  }

  @Test
  public void testUploadGlobalState_updateResource() throws IOException {
    byte[] data = "file_data_1".getBytes(StandardCharsets.UTF_8);
    s3Backend.uploadGlobalState("upload_global_service_1", data);

    String prefix = S3Backend.getGlobalStateResourcePrefix("upload_global_service_1");
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertGlobalStateVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadGlobalState("upload_global_service_1", data);

    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertGlobalStateVersion(contents);

    versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
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
    assertTrue(TimeStringUtils.isTimeStringSec(splits[0]));
    UUID.fromString(splits[1]);
  }

  @Test
  public void testDownloadIndexState_singlePart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_index_state_service", "download_index", IndexResourceType.INDEX_STATE);
    String fileName = S3Backend.getIndexStateFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(prefix + fileName)
            .build(),
        RequestBody.fromString("resource_data"));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName).build(),
        RequestBody.fromString("resource_data_1"));

    InputStream downloadStream =
        s3Backend.downloadIndexState("download_index_state_service_4", "download_index_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getIndexStateFileName();
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName2).build(),
        RequestBody.fromString("resource_data_2"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
        RequestBody.fromString(fileName2));

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
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertIndexStateVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
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
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertIndexStateVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadIndexState("upload_index_state_service_1", "upload_index_1", data);

    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertIndexStateVersion(contents);

    versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
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
    assertTrue(TimeStringUtils.isTimeStringSec(splits[0]));
    UUID.fromString(splits[1]);
  }

  @Test
  public void testDownloadWarmingQueries_singlePart() throws IOException {
    String prefix =
        S3Backend.getIndexResourcePrefix(
            "download_warming_service", "download_index", IndexResourceType.WARMING_QUERIES);
    String fileName = S3Backend.getWarmingQueriesFileName();
    String currentKey = prefix + S3Backend.CURRENT_VERSION;
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(prefix + fileName)
            .build(),
        RequestBody.fromString("resource_data"));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));

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
    s3.putObject(
        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(currentKey)
            .build(),
        RequestBody.fromString(fileName));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName).build(),
        RequestBody.fromString("resource_data_1"));

    InputStream downloadStream =
        s3Backend.downloadWarmingQueries("download_warming_service_4", "download_index_4");
    assertEquals("resource_data_1", convertToString(downloadStream));

    // update resource data
    String fileName2 = S3Backend.getWarmingQueriesFileName();
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(prefix + fileName2).build(),
        RequestBody.fromString("resource_data_2"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
        RequestBody.fromString(fileName2));

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
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertWarmingVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
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
    String contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertWarmingVersion(contents);

    String versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
    assertEquals("file_data_1", contents);

    // update data
    data = "file_data_2".getBytes(StandardCharsets.UTF_8);

    s3Backend.uploadWarmingQueries("upload_warming_service_1", "upload_index_1", data);

    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(currentKey).build(),
                ResponseTransformer.toInputStream()));
    assertWarmingVersion(contents);

    versionKey = prefix + contents;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(versionKey).build(),
                ResponseTransformer.toInputStream()));
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
    assertTrue(TimeStringUtils.isTimeStringSec(splits[0]));
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
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename1).build(),
                ResponseTransformer.toInputStream()));
    assertEquals("file1_data", contents);
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename2).build(),
                ResponseTransformer.toInputStream()));
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

    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename1).build(),
        RequestBody.fromString("file1_data"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename2).build(),
        RequestBody.fromString("file2_data"));

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

    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename1).build(),
        RequestBody.fromString("file1_data"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + filename2).build(),
        RequestBody.fromString("file2_data"));

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
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + fileName).build(),
                ResponseTransformer.toInputStream()));
    assertEquals(getPointStateJson(), contents);

    String latestKey = keyPrefix + S3Backend.CURRENT_VERSION;
    contents =
        convertToString(
            s3.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(latestKey).build(),
                ResponseTransformer.toInputStream()));
    assertEquals(fileName, contents);
  }

  @Test
  public void testDownloadPointState() throws IOException {
    NrtPointState pointState = getPointState();
    String keyPrefix =
        S3Backend.getIndexResourcePrefix(
            "download_point_service", "download_point_index", IndexResourceType.POINT_STATE);
    String fileName = S3Backend.getPointStateFileName(pointState);
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyPrefix + fileName).build(),
        RequestBody.fromString(getPointStateJson()));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(keyPrefix + S3Backend.CURRENT_VERSION)
            .build(),
        RequestBody.fromString(fileName));

    RemoteBackend.InputStreamWithTimestamp inputStreamWithTimestamp =
        s3Backend.downloadPointState("download_point_service", "download_point_index", null);
    byte[] downloadedData = inputStreamWithTimestamp.inputStream().readAllBytes();
    NrtPointState downloadedPointState = RemoteUtils.pointStateFromUtf8(downloadedData);
    assertEquals(pointState, downloadedPointState);

    Instant pointStateTimestamp =
        TimeStringUtils.parseTimeStringSec(S3Backend.getTimeStringFromPointStateFileName(fileName));
    assertEquals(pointStateTimestamp, inputStreamWithTimestamp.timestamp());
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
    assertTrue(TimeStringUtils.isTimeStringSec(components[0]));
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
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(prefix + S3Backend.CURRENT_VERSION)
            .build(),
        RequestBody.fromString("current_version_name"));
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
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(prefix + S3Backend.CURRENT_VERSION)
                    .build(),
                ResponseTransformer.toInputStream()));
    assertEquals("current_version_name", contents);
  }

  @Test
  public void testCurrentResourceExists() {
    String prefix = "service_current_exists/resource/";
    assertFalse(s3Backend.currentResourceExists(prefix));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(prefix + S3Backend.CURRENT_VERSION)
            .build(),
        RequestBody.fromString("current_version_name"));
    assertTrue(s3Backend.currentResourceExists(prefix));
  }

  @Test
  public void testDownloadIndexFile() throws IOException {
    String service = "test_service";
    String indexIdentifier = "test_index";
    String fileName = "test_file.dat";
    String testContent = "test file content for download";

    NrtFileMetaData fileMetaData =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "test_primary_id", "20240827120000");

    // Set up the S3 object with the expected backend file name and path
    String backendFileName = S3Backend.getIndexBackendFileName(fileName, fileMetaData);
    String backendPrefix = S3Backend.getIndexDataPrefix(service, indexIdentifier);
    String backendKey = backendPrefix + backendFileName;

    // Put the test content in S3
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(backendKey).build(),
        RequestBody.fromString(testContent));

    // Download the file using the method under test
    InputStream downloadStream =
        s3Backend.downloadIndexFile(service, indexIdentifier, fileName, fileMetaData);
    String downloadedContent = convertToString(downloadStream);

    assertEquals(testContent, downloadedContent);
  }

  @Test
  public void testDownloadIndexFile_notFound() throws IOException {
    String service = "test_service_not_found";
    String indexIdentifier = "test_index_not_found";
    String fileName = "non_existent_file.dat";

    NrtFileMetaData fileMetaData =
        new NrtFileMetaData(new byte[0], new byte[0], 1, 0, "test_primary_id", "20240827120000");

    try {
      s3Backend.downloadIndexFile(service, indexIdentifier, fileName, fileMetaData);
      fail("Expected IllegalArgumentException for non-existent file");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }

  @Test
  public void testGetTimeStringFromPointStateFileName() {
    // Test valid point state file names with different formats
    String timeString1 = "20240827120000";
    String primaryId1 = "primary123";
    String gen1 = "1";
    String fileName1 = timeString1 + "-" + primaryId1 + "-" + gen1;

    assertEquals(timeString1, S3Backend.getTimeStringFromPointStateFileName(fileName1));

    // Test with different time string
    String timeString2 = "20241225153045";
    String primaryId2 = "anotherPrimary";
    String gen2 = "5";
    String fileName2 = timeString2 + "-" + primaryId2 + "-" + gen2;

    assertEquals(timeString2, S3Backend.getTimeStringFromPointStateFileName(fileName2));

    // Test with longer file name (more parts)
    String fileName3 = timeString1 + "-" + primaryId1 + "-" + gen1 + "-extra-parts";
    assertEquals(timeString1, S3Backend.getTimeStringFromPointStateFileName(fileName3));
  }

  @Test
  public void testGetTimeStringFromPointStateFileName_invalidFormat() {
    // Test with insufficient parts (less than 3)
    try {
      S3Backend.getTimeStringFromPointStateFileName("onlyonepart");
      fail("Expected IllegalArgumentException for file name with insufficient parts");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid point state file name: onlyonepart", e.getMessage());
    }

    try {
      S3Backend.getTimeStringFromPointStateFileName("only-two");
      fail("Expected IllegalArgumentException for file name with insufficient parts");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid point state file name: only-two", e.getMessage());
    }

    // Test with empty string
    try {
      S3Backend.getTimeStringFromPointStateFileName("");
      fail("Expected IllegalArgumentException for empty file name");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid point state file name: ", e.getMessage());
    }
  }

  @Test
  public void testGetTimeStringFromPointStateFileName_realWorldExample() {
    // Test with actual point state file name from getPointState method
    NrtPointState pointState = getPointState();
    String fileName = S3Backend.getPointStateFileName(pointState);

    // Extract time string and verify it's valid
    String extractedTimeString = S3Backend.getTimeStringFromPointStateFileName(fileName);

    // Verify the extracted time string is a valid time string format
    assertTrue(
        "Extracted time string should be valid",
        TimeStringUtils.isTimeStringSec(extractedTimeString));

    // Verify the filename starts with the extracted time string
    assertTrue(
        "Filename should start with time string", fileName.startsWith(extractedTimeString + "-"));
  }

  @Test
  public void testWrapDownloadStream_withMetricsNoRateLimiter() {
    // Create a mock InputStream
    InputStream mockInputStream = mock(InputStream.class);
    String indexIdentifier = "test_index";

    // Call wrapDownloadStream with metrics enabled but no rate limiter
    InputStream result = S3Backend.wrapDownloadStream(mockInputStream, true, indexIdentifier, null);

    // Verify the result is an S3DownloadStreamWrapper
    assertTrue(
        "Result should be an S3DownloadStreamWrapper", result instanceof S3DownloadStreamWrapper);

    // Verify it's not a GlobalThrottledInputStream
    assertFalse(
        "Result should not be a GlobalThrottledInputStream",
        result instanceof GlobalThrottledInputStream);
  }

  @Test
  public void testWrapDownloadStream_withRateLimiterNoMetrics() {
    // Create a mock InputStream
    InputStream mockInputStream = mock(InputStream.class);
    String indexIdentifier = "test_index";

    // Create a rate limiter
    GlobalWindowRateLimiter rateLimiter = new GlobalWindowRateLimiter(1024, 1);

    // Call wrapDownloadStream with rate limiter enabled but no metrics
    InputStream result =
        S3Backend.wrapDownloadStream(mockInputStream, false, indexIdentifier, rateLimiter);

    // Verify the result is a GlobalThrottledInputStream
    assertTrue(
        "Result should be a GlobalThrottledInputStream",
        result instanceof GlobalThrottledInputStream);

    // Verify it's not an S3DownloadStreamWrapper
    assertFalse(
        "Result should not be an S3DownloadStreamWrapper",
        result instanceof S3DownloadStreamWrapper);
  }

  @Test
  public void testWrapDownloadStream_withBothMetricsAndRateLimiter() {
    // Create a mock InputStream
    InputStream mockInputStream = mock(InputStream.class);
    String indexIdentifier = "test_index";

    // Create a rate limiter
    GlobalWindowRateLimiter rateLimiter = new GlobalWindowRateLimiter(1024, 1);

    // Call wrapDownloadStream with both metrics and rate limiter enabled
    InputStream result =
        S3Backend.wrapDownloadStream(mockInputStream, true, indexIdentifier, rateLimiter);

    // The result should be a GlobalThrottledInputStream wrapping an S3DownloadStreamWrapper
    assertTrue(
        "Result should be a GlobalThrottledInputStream",
        result instanceof GlobalThrottledInputStream);

    // We can't directly check the wrapped stream type without reflection or exposing it,
    // but we can verify the behavior is as expected by checking the class name
    String className = result.getClass().getName();
    assertEquals("com.yelp.nrtsearch.server.utils.GlobalThrottledInputStream", className);
  }

  @Test
  public void testWrapDownloadStream_withNeitherMetricsNorRateLimiter() {
    // Create a mock InputStream
    InputStream mockInputStream = mock(InputStream.class);
    String indexIdentifier = "test_index";

    // Call wrapDownloadStream with neither metrics nor rate limiter enabled
    InputStream result =
        S3Backend.wrapDownloadStream(mockInputStream, false, indexIdentifier, null);

    // The result should be the original input stream
    assertEquals("Result should be the original input stream", mockInputStream, result);

    // Verify it's not an S3DownloadStreamWrapper or GlobalThrottledInputStream
    assertFalse(
        "Result should not be an S3DownloadStreamWrapper",
        result instanceof S3DownloadStreamWrapper);
    assertFalse(
        "Result should not be a GlobalThrottledInputStream",
        result instanceof GlobalThrottledInputStream);
  }

  private void putMultiPart(String key, List<String> partsData) {
    CreateMultipartUploadRequest initRequest =
        CreateMultipartUploadRequest.builder().bucket(BUCKET_NAME).key(key).build();
    CreateMultipartUploadResponse initResponse = s3.createMultipartUpload(initRequest);

    List<CompletedPart> completedParts = new ArrayList<>();
    for (int i = 0; i < partsData.size(); ++i) {
      UploadPartRequest uploadRequest =
          UploadPartRequest.builder()
              .bucket(BUCKET_NAME)
              .key(key)
              .uploadId(initResponse.uploadId())
              .partNumber(i + 1)
              .build();

      UploadPartResponse uploadResult =
          s3.uploadPart(uploadRequest, RequestBody.fromBytes(partsData.get(i).getBytes()));
      completedParts.add(
          CompletedPart.builder().partNumber(i + 1).eTag(uploadResult.eTag()).build());
    }

    CompleteMultipartUploadRequest compRequest =
        CompleteMultipartUploadRequest.builder()
            .bucket(BUCKET_NAME)
            .key(key)
            .uploadId(initResponse.uploadId())
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build();
    s3.completeMultipartUpload(compRequest);
  }

  private String convertToString(InputStream inputStream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
    return writer.toString();
  }
}
