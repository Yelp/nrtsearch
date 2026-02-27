/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.legacy;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class LegacyVersionManagerTest {
  private final String BUCKET_NAME = "version-manager-unittest";
  private LegacyVersionManager versionManager;
  private S3Client s3;
  private Path archiverDirectory;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(BUCKET_NAME);

  @Before
  public void setup() throws IOException {
    archiverDirectory = folder.newFolder("version-manager").toPath();

    s3 = s3Provider.getAmazonS3();
    versionManager = new LegacyVersionManager(s3, BUCKET_NAME);
  }

  @Test
  public void blessVersionNoResourceHash() {
    boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
    assertEquals(false, result);
  }

  @Test
  public void blessVersionWhenNoPrior() throws IOException {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/testresource/abcdef")
            .build(),
        RequestBody.fromString("foo"));
    boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
    assertEquals(true, result);

    ResponseInputStream<GetObjectResponse> response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/_latest_version")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("0", new String(response.readAllBytes()));

    response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/0")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("abcdef", new String(response.readAllBytes()));
  }

  @Test
  public void blessVersionWhenPrior() throws IOException {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/testresource/abcdef")
            .build(),
        RequestBody.fromString("foo"));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/_version/testresource/_latest_version")
            .build(),
        RequestBody.fromString("0"));
    boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
    assertEquals(true, result);

    ResponseInputStream<GetObjectResponse> response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/_latest_version")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("1", new String(response.readAllBytes()));

    response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/1")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("abcdef", new String(response.readAllBytes()));
  }

  @Test
  public void blessVersionWhenLatestVersionBehind() throws IOException {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/testresource/abcdef")
            .build(),
        RequestBody.fromString("foo"));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/_version/testresource/_latest_version")
            .build(),
        RequestBody.fromString("0"));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/_version/testresource/1")
            .build(),
        RequestBody.fromString("ghijkl"));

    boolean result = versionManager.blessVersion("testservice", "testresource", "abcdef");
    assertEquals(true, result);

    ResponseInputStream<GetObjectResponse> response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/_latest_version")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("2", new String(response.readAllBytes()));

    response =
        s3.getObject(
            GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("testservice/_version/testresource/2")
                .build(),
            ResponseTransformer.toInputStream());
    assertEquals("abcdef", new String(response.readAllBytes()));
  }

  @Test
  public void deleteVersionWhenDoesntExist() throws IOException {
    boolean result = versionManager.deleteVersion("testservice", "testresource", "abcdef");
    assertEquals(false, result);
  }

  @Test
  public void deleteVersionWhenExists() throws IOException {
    String key1 = "testservice/testresource/abcdef";
    String key2 = "testservice/testresource/other_version";
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(key1).build(),
        RequestBody.fromString("foo"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(key2).build(),
        RequestBody.fromString("boo"));
    boolean result = versionManager.deleteVersion("testservice", "testresource", "abcdef");
    assertEquals(true, result);

    List<S3Object> objects =
        s3.listObjects(
                ListObjectsRequest.builder()
                    .bucket(BUCKET_NAME)
                    .prefix("testservice/testresource/")
                    .build())
            .contents();

    List<String> objectKeys = objects.stream().map(S3Object::key).collect(Collectors.toList());

    assertTrue(objectKeys.contains(key2));
    assertFalse(objectKeys.contains(key1));
  }

  @Test
  public void getVersionString() throws IOException {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/_version/testresource/versionHash")
            .build(),
        RequestBody.fromString("0"));
    String result = versionManager.getVersionString("testservice", "testresource", "versionHash");
    assertEquals("0", result);

    s3.putObject(
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key("testservice/_version/testresource/versionHash")
            .build(),
        RequestBody.fromString("2"));
    result = versionManager.getVersionString("testservice", "testresource", "versionHash");
    assertEquals("2", result);
  }
}
