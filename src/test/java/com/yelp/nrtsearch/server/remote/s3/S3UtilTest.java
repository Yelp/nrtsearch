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

import static com.yelp.nrtsearch.server.remote.s3.S3Util.getS3FileName;
import static com.yelp.nrtsearch.server.remote.s3.S3Util.isValidS3FilePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.remote.s3.S3Util.S3CrtConfig;
import com.yelp.nrtsearch.server.remote.s3.S3Util.S3JavaAsyncConfig;
import java.io.ByteArrayInputStream;
import org.junit.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3UtilTest {

  @Test
  public void testIsValidS3FilePath() {
    assertTrue(isValidS3FilePath("s3://random-bucket/path1"));
    assertTrue(isValidS3FilePath("s3://random-bucket/path1/path2"));
    assertTrue(isValidS3FilePath("s3://random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("s3://random-bucket/path1/path2/"));
    assertFalse(isValidS3FilePath("s3://random-bucket"));
    assertFalse(isValidS3FilePath("/random-bucket/path1"));
    assertFalse(isValidS3FilePath("/random-bucket/path1/path2"));
    assertFalse(isValidS3FilePath("/random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("C://random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("~/random-bucket/path1/path2/path3.txt"));
    assertFalse(isValidS3FilePath("http://random-bucket/path1/path2/path3.txt"));
  }

  @Test
  public void testGetS3FileName() {
    assertEquals("path1", getS3FileName("s3://random-bucket/path1"));
    assertEquals("path2", getS3FileName("s3://random-bucket/path1/path2"));
    assertEquals("path3.txt", getS3FileName("s3://random-bucket/path1/path2/path3.txt"));
    assertThrows(IllegalArgumentException.class, () -> getS3FileName("s3://random-bucket"));
    assertThrows(
        IllegalArgumentException.class, () -> getS3FileName("s3://random-bucket/path1/path2/"));
  }

  /**
   * Verifies that buildS3ClientBundle returns a dummy bundle (null async client) when
   * getBucketLocation() throws the given SdkException subtype.
   *
   * <p>Call flow on the error path in buildS3ClientBundle:
   *
   * <ol>
   *   <li>Builder A — line 78: creates clientBuilder (never .build()-ed on error path)
   *   <li>Builder B — line 87: interim client; its .build() returns a mock whose
   *       getBucketLocation() throws the supplied exception
   *   <li>Builder C — line 113: dummy client builder inside the catch block; its .build() returns
   *       the dummy S3Client
   * </ol>
   */
  private void verifyDummyBundleReturnedWhenGetBucketLocationThrows(SdkException exception) {
    NrtsearchConfig config = mock(NrtsearchConfig.class);
    when(config.getBotoCfgPath()).thenReturn(null);
    when(config.getBucketName()).thenReturn("nonexistent-bucket");
    when(config.getEnableGlobalBucketAccess()).thenReturn(false);

    // Builder A: returned for the first S3Client.builder() call (line 78, clientBuilder)
    S3ClientBuilder builderA = mock(S3ClientBuilder.class, RETURNS_SELF);

    // Builder B: returned for the second S3Client.builder() call (line 87, interim client)
    S3Client interimClient = mock(S3Client.class);
    when(interimClient.getBucketLocation(
            any(software.amazon.awssdk.services.s3.model.GetBucketLocationRequest.class)))
        .thenThrow(exception);
    S3ClientBuilder builderB = mock(S3ClientBuilder.class, RETURNS_SELF);
    when(builderB.build()).thenReturn(interimClient);

    // Builder C: returned for the third S3Client.builder() call (line 113, dummy client)
    S3Client dummyClient = mock(S3Client.class);
    S3ClientBuilder builderC = mock(S3ClientBuilder.class, RETURNS_SELF);
    when(builderC.build()).thenReturn(dummyClient);

    try (MockedStatic<S3Client> mockedS3Client = mockStatic(S3Client.class)) {
      mockedS3Client
          .when(S3Client::builder)
          .thenReturn(builderA)
          .thenReturn(builderB)
          .thenReturn(builderC);

      S3Util.S3ClientBundle bundle = S3Util.buildS3ClientBundle(config);

      assertSame(dummyClient, bundle.s3Client());
      assertNull(bundle.s3AsyncClient());
    }
  }

  @Test
  public void testBuildS3ClientBundle_SdkClientException_returnsDummyBundle() {
    verifyDummyBundleReturnedWhenGetBucketLocationThrows(
        SdkClientException.create("simulated client exception"));
  }

  @Test
  public void testBuildS3ClientBundle_NoSuchBucketException_returnsDummyBundle() {
    verifyDummyBundleReturnedWhenGetBucketLocationThrows(
        NoSuchBucketException.builder().message("nonexistent-bucket").build());
  }

  @Test
  public void testBuildS3ClientBundle_S3Exception_returnsDummyBundle() {
    verifyDummyBundleReturnedWhenGetBucketLocationThrows(
        S3Exception.builder().message("Access Denied").statusCode(403).build());
  }

  @Test
  public void testBuildS3ClientBundle_SdkServiceException_returnsDummyBundle() {
    verifyDummyBundleReturnedWhenGetBucketLocationThrows(
        SdkServiceException.builder().message("service error").statusCode(500).build());
  }

  @Test
  public void testS3JavaAsyncConfig_defaults() {
    String configStr = "bucketName: test-bucket";
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    S3JavaAsyncConfig javaConfig = S3JavaAsyncConfig.fromConfig(config);

    assertEquals(8 * 1024 * 1024L, javaConfig.getMinimumPartSizeInBytes());
    assertEquals(8 * 1024 * 1024L, javaConfig.getThresholdSizeInBytes());
    assertEquals(0L, javaConfig.getApiCallBufferSizeInBytes());
    assertEquals(0, javaConfig.getMaxInFlightParts());
    assertEquals(0, javaConfig.getIoThreads());
  }

  @Test
  public void testS3JavaAsyncConfig_customValues() {
    String configStr =
        "bucketName: test-bucket\n"
            + "remoteConfig:\n"
            + "  s3:\n"
            + "    java:\n"
            + "      minimumPartSize: 16mb\n"
            + "      thresholdSize: 32mb\n"
            + "      apiCallBufferSize: 64mb\n"
            + "      maxInFlightParts: 8\n"
            + "      ioThreads: 16\n";
    NrtsearchConfig config = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    S3JavaAsyncConfig javaConfig = S3JavaAsyncConfig.fromConfig(config);

    assertEquals(16 * 1024 * 1024L, javaConfig.getMinimumPartSizeInBytes());
    assertEquals(32 * 1024 * 1024L, javaConfig.getThresholdSizeInBytes());
    assertEquals(64 * 1024 * 1024L, javaConfig.getApiCallBufferSizeInBytes());
    assertEquals(8, javaConfig.getMaxInFlightParts());
    assertEquals(16, javaConfig.getIoThreads());
  }

  @Test
  public void testS3JavaAsyncConfig_invalidMinimumPartSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3JavaAsyncConfig(0L, 8 * 1024 * 1024L, 0L, 0, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3JavaAsyncConfig(-1L, 8 * 1024 * 1024L, 0L, 0, 0));
  }

  @Test
  public void testS3JavaAsyncConfig_invalidThresholdSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3JavaAsyncConfig(8 * 1024 * 1024L, 0L, 0L, 0, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3JavaAsyncConfig(8 * 1024 * 1024L, -1L, 0L, 0, 0));
  }

  @Test
  public void testSizeStrToBytes() {
    assertEquals(1024L, S3CrtConfig.sizeStrToBytes("1kb"));
    assertEquals(1024L, S3CrtConfig.sizeStrToBytes("1KB"));
    assertEquals(8 * 1024 * 1024L, S3CrtConfig.sizeStrToBytes("8mb"));
    assertEquals(2L * 1024 * 1024 * 1024, S3CrtConfig.sizeStrToBytes("2gb"));
    assertEquals(1048576L, S3CrtConfig.sizeStrToBytes("1048576"));
    assertThrows(IllegalArgumentException.class, () -> S3CrtConfig.sizeStrToBytes("mb"));
    assertThrows(NumberFormatException.class, () -> S3CrtConfig.sizeStrToBytes("abc"));
  }

  @Test
  public void testInvalidAsyncClientType() {
    NrtsearchConfig config = mock(NrtsearchConfig.class);
    when(config.getBotoCfgPath()).thenReturn(null);
    when(config.getBucketName()).thenReturn("test-bucket");
    when(config.getEnableGlobalBucketAccess()).thenReturn(false);
    when(config.getMaxS3ClientRetries()).thenReturn(0);

    com.yelp.nrtsearch.server.config.YamlConfigReader configReader =
        mock(com.yelp.nrtsearch.server.config.YamlConfigReader.class);
    when(config.getConfigReader()).thenReturn(configReader);
    when(configReader.getString("remoteConfig.s3.asyncClientType", "crt"))
        .thenReturn("unknown_type");

    // The final sync S3Client built from builderA.build()
    software.amazon.awssdk.services.s3.S3ServiceClientConfiguration serviceClientConfig =
        mock(software.amazon.awssdk.services.s3.S3ServiceClientConfiguration.class);
    when(serviceClientConfig.region()).thenReturn(software.amazon.awssdk.regions.Region.US_EAST_1);
    S3Client finalS3Client = mock(S3Client.class);
    when(finalS3Client.serviceClientConfiguration()).thenReturn(serviceClientConfig);

    // Builder A: clientBuilder — its .build() returns the final sync client
    S3ClientBuilder builderA = mock(S3ClientBuilder.class, RETURNS_SELF);
    when(builderA.build()).thenReturn(finalS3Client);

    // Builder B: interim client — getBucketLocation succeeds
    S3Client interimClient = mock(S3Client.class);
    GetBucketLocationResponse locationResponse =
        GetBucketLocationResponse.builder().locationConstraint("us-east-1").build();
    when(interimClient.getBucketLocation(
            any(software.amazon.awssdk.services.s3.model.GetBucketLocationRequest.class)))
        .thenReturn(locationResponse);
    S3ClientBuilder builderB = mock(S3ClientBuilder.class, RETURNS_SELF);
    when(builderB.build()).thenReturn(interimClient);

    try (MockedStatic<S3Client> mockedS3Client = mockStatic(S3Client.class)) {
      mockedS3Client.when(S3Client::builder).thenReturn(builderA).thenReturn(builderB);

      assertThrows(IllegalArgumentException.class, () -> S3Util.buildS3ClientBundle(config));
    }
  }
}
