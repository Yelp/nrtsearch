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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.remote.s3.S3Backend.S3BackendConfig;
import com.yelp.nrtsearch.server.utils.GlobalWindowRateLimiter;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3BackendRateLimitTest {
  private static final String BUCKET_NAME = "s3-backend-rate-limit-test";
  private static final String KEY = "test_key";
  private static final String CONTENT = "test_content";

  @ClassRule public static final AmazonS3Provider S3_PROVIDER = new AmazonS3Provider(BUCKET_NAME);

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static S3Client s3;

  @BeforeClass
  public static void setup() {
    s3 = S3_PROVIDER.getAmazonS3();
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(KEY).build(),
        RequestBody.fromString(CONTENT));
  }

  @AfterClass
  public static void cleanUp() {
    // Don't shut down the S3 client here as it's shared across tests
    // The AmazonS3Provider will handle cleanup
  }

  @Test
  public void testRateLimiterInitialization_NoRateLimit() throws Exception {
    // Create S3Backend with default config (no rate limiting)
    String configStr = "bucketName: " + BUCKET_NAME;
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    S3Backend s3Backend =
        new S3Backend(
            nrtsearchConfig, new S3Util.S3ClientBundle(s3, S3_PROVIDER.getS3AsyncClient()));

    // Use reflection to access the private rateLimiter field
    Field rateLimiterField = S3Backend.class.getDeclaredField("rateLimiter");
    rateLimiterField.setAccessible(true);
    GlobalWindowRateLimiter rateLimiter = (GlobalWindowRateLimiter) rateLimiterField.get(s3Backend);

    // Verify rateLimiter is null when rate limit is 0
    assertNull("RateLimiter should be null when rate limit is 0", rateLimiter);
  }

  @Test
  public void testRateLimiterInitialization_WithRateLimit() throws Exception {
    // Create S3Backend with rate limiting config
    String configStr =
        "bucketName: "
            + BUCKET_NAME
            + "\n"
            + "remoteConfig:\n"
            + "  s3:\n"
            + "    rateLimitPerSecond: 1MB\n"
            + "    rateLimitWindowSeconds: 5";
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    S3Backend s3Backend =
        new S3Backend(
            nrtsearchConfig, new S3Util.S3ClientBundle(s3, S3_PROVIDER.getS3AsyncClient()));

    // Use reflection to access the private rateLimiter field
    Field rateLimiterField = S3Backend.class.getDeclaredField("rateLimiter");
    rateLimiterField.setAccessible(true);
    GlobalWindowRateLimiter rateLimiter = (GlobalWindowRateLimiter) rateLimiterField.get(s3Backend);

    // Verify rateLimiter is not null when rate limit is > 0
    assertNotNull("RateLimiter should not be null when rate limit is > 0", rateLimiter);

    // Verify rateLimiter is initialized with correct values
    // Access private fields of GlobalWindowRateLimiter using reflection
    Field bytesPerWindowField = GlobalWindowRateLimiter.class.getDeclaredField("bytesPerWindow");
    bytesPerWindowField.setAccessible(true);
    long bytesPerWindow = (long) bytesPerWindowField.get(rateLimiter);

    Field windowMillisField = GlobalWindowRateLimiter.class.getDeclaredField("windowMillis");
    windowMillisField.setAccessible(true);
    long windowMillis = (long) windowMillisField.get(rateLimiter);

    // Verify the values match what we configured
    assertEquals(1024 * 1024 * 5, bytesPerWindow); // 1MB * 5 seconds
    assertEquals(5000, windowMillis); // 5 seconds in milliseconds
  }

  @Test
  public void testDownloadWithRateLimit() throws IOException {
    // Create S3Backend with rate limiting config
    String configStr =
        "bucketName: "
            + BUCKET_NAME
            + "\n"
            + "remoteConfig:\n"
            + "  s3:\n"
            + "    rateLimitPerSecond: 1MB\n"
            + "    rateLimitWindowSeconds: 5";
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    S3Backend s3Backend =
        new S3Backend(
            nrtsearchConfig, new S3Util.S3ClientBundle(s3, S3_PROVIDER.getS3AsyncClient()));

    // Download a file - this should work normally since our test file is small
    // and the rate limit is high enough
    InputStream inputStream =
        s3Backend.downloadFromS3Path(String.format("s3://%s/%s", BUCKET_NAME, KEY));
    try {
      String contentFromS3 = convertToString(inputStream);
      assertEquals(CONTENT, contentFromS3);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  @Test
  public void testCustomRateLimitConfig() throws Exception {
    // Create a custom S3BackendConfig
    S3BackendConfig customConfig =
        new S3BackendConfig(
            false, 2 * 1024 * 1024, 10); // 2MB/s, 10 second window, metrics disabled

    // Create S3Backend with custom config
    S3Backend s3Backend =
        new S3Backend(
            BUCKET_NAME,
            false,
            customConfig,
            new S3Util.S3ClientBundle(s3, S3_PROVIDER.getS3AsyncClient()));

    // Use reflection to access the private rateLimiter field
    Field rateLimiterField = S3Backend.class.getDeclaredField("rateLimiter");
    rateLimiterField.setAccessible(true);
    GlobalWindowRateLimiter rateLimiter = (GlobalWindowRateLimiter) rateLimiterField.get(s3Backend);

    // Verify rateLimiter is not null
    assertNotNull("RateLimiter should not be null with custom config", rateLimiter);

    // Verify rateLimiter is initialized with correct values
    Field bytesPerWindowField = GlobalWindowRateLimiter.class.getDeclaredField("bytesPerWindow");
    bytesPerWindowField.setAccessible(true);
    long bytesPerWindow = (long) bytesPerWindowField.get(rateLimiter);

    Field windowMillisField = GlobalWindowRateLimiter.class.getDeclaredField("windowMillis");
    windowMillisField.setAccessible(true);
    long windowMillis = (long) windowMillisField.get(rateLimiter);

    // Verify the values match what we configured
    assertEquals(2 * 1024 * 1024 * 10, bytesPerWindow); // 2MB * 10 seconds
    assertEquals(10000, windowMillis); // 10 seconds in milliseconds
  }

  @Test
  public void testMetricsConfig() throws Exception {
    // Create a custom S3BackendConfig with metrics enabled
    S3BackendConfig customConfig =
        new S3BackendConfig(true, 0, 1); // No rate limit, metrics enabled

    // Create S3Backend with custom config
    S3Backend s3Backend =
        new S3Backend(
            BUCKET_NAME,
            false,
            customConfig,
            new S3Util.S3ClientBundle(s3, S3_PROVIDER.getS3AsyncClient()));

    // Use reflection to access the private s3Metrics field
    Field s3MetricsField = S3Backend.class.getDeclaredField("s3Metrics");
    s3MetricsField.setAccessible(true);
    boolean s3Metrics = (boolean) s3MetricsField.get(s3Backend);

    // Verify s3Metrics is true
    assertEquals(true, s3Metrics);

    // Download a file and verify metrics wrapper is used
    InputStream inputStream =
        s3Backend.downloadFromS3Path(String.format("s3://%s/%s", BUCKET_NAME, KEY));
    try {
      // Verify the stream is wrapped with S3DownloadStreamWrapper
      assertEquals(
          "com.yelp.nrtsearch.server.monitoring.S3DownloadStreamWrapper",
          inputStream.getClass().getName());

      String contentFromS3 = convertToString(inputStream);
      assertEquals(CONTENT, contentFromS3);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  private String convertToString(InputStream inputStream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
    return writer.toString();
  }
}
