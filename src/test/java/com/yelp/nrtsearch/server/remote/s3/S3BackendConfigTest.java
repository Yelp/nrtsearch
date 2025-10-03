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
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.remote.s3.S3Backend.S3BackendConfig;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class S3BackendConfigTest {

  @Test
  public void testConstructorAndGetters() {
    // Test constructor with rate limit values and metrics flag
    long bytesPerSecond = 1024 * 1024; // 1MB
    int windowSeconds = 5;
    boolean metrics = true;
    S3BackendConfig config = new S3BackendConfig(metrics, bytesPerSecond, windowSeconds);

    // Verify getters return expected values
    assertEquals(bytesPerSecond, config.getRateLimitBytes());
    assertEquals(windowSeconds, config.getRateLimitWindowSeconds());
    assertEquals(metrics, config.getMetrics());
  }

  @Test
  public void testFromConfig_NoRateLimit() {
    // Create config with no rate limit settings
    String configStr = "bucketName: test-bucket";
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));

    // Create S3BackendConfig from NrtsearchConfig
    S3BackendConfig config = S3BackendConfig.fromConfig(nrtsearchConfig);

    // Verify no rate limit is set (0 bytes, but default window of 1 second)
    assertEquals(0, config.getRateLimitBytes());
    assertEquals(1, config.getRateLimitWindowSeconds()); // Default is 1, not 0
    assertEquals(false, config.getMetrics()); // Default metrics is false
  }

  @Test
  public void testFromConfig_WithRateLimit() {
    // Create config with rate limit settings
    String configStr =
        "bucketName: test-bucket\n"
            + "remoteConfig:\n"
            + "  s3:\n"
            + "    rateLimitPerSecond: 1MB\n"
            + "    rateLimitWindowSeconds: 5";
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));

    // Create S3BackendConfig from NrtsearchConfig
    S3BackendConfig config = S3BackendConfig.fromConfig(nrtsearchConfig);

    // Verify rate limit is set correctly
    assertEquals(1024 * 1024, config.getRateLimitBytes());
    assertEquals(5, config.getRateLimitWindowSeconds());
    assertEquals(false, config.getMetrics()); // Default metrics is false when not specified
  }

  @Test
  public void testFromConfig_WithMetrics() {
    // Create config with metrics setting
    String configStr =
        "bucketName: test-bucket\n" + "remoteConfig:\n" + "  s3:\n" + "    metrics: true";
    NrtsearchConfig nrtsearchConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));

    // Create S3BackendConfig from NrtsearchConfig
    S3BackendConfig config = S3BackendConfig.fromConfig(nrtsearchConfig);

    // Verify metrics is set correctly
    assertEquals(true, config.getMetrics());
    assertEquals(0, config.getRateLimitBytes()); // Default rate limit is 0
    assertEquals(1, config.getRateLimitWindowSeconds()); // Default window is 1
  }

  @Test
  public void testRateLimitStringToBytes_KB() {
    // Test KB conversion
    assertEquals(1024, S3BackendConfig.rateLimitStringToBytes("1KB"));
    assertEquals(1024 * 5, S3BackendConfig.rateLimitStringToBytes("5KB"));
  }

  @Test
  public void testRateLimitStringToBytes_MB() {
    // Test MB conversion
    assertEquals(1024 * 1024, S3BackendConfig.rateLimitStringToBytes("1MB"));
    assertEquals(1024 * 1024 * 10, S3BackendConfig.rateLimitStringToBytes("10MB"));
  }

  @Test
  public void testRateLimitStringToBytes_GB() {
    // Test GB conversion - use long literals to avoid integer overflow
    assertEquals(1024L * 1024L * 1024L, S3BackendConfig.rateLimitStringToBytes("1GB"));
    assertEquals(1024L * 1024L * 1024L * 2L, S3BackendConfig.rateLimitStringToBytes("2GB"));
  }

  @Test
  public void testRateLimitStringToBytes_NumericOnly() {
    // Test numeric only (bytes)
    assertEquals(1000, S3BackendConfig.rateLimitStringToBytes("1000"));
  }

  @Test
  public void testRateLimitStringToBytes_Invalid() {
    // Test invalid format throws NumberFormatException
    try {
      S3BackendConfig.rateLimitStringToBytes("invalid");
      fail("Expected NumberFormatException");
    } catch (NumberFormatException e) {
      // Expected
    }

    try {
      S3BackendConfig.rateLimitStringToBytes("1XB");
      fail("Expected NumberFormatException");
    } catch (NumberFormatException e) {
      // Expected
    }
  }

  @Test
  public void testRateLimitStringToBytes_Null() {
    // Test null throws NullPointerException
    try {
      S3BackendConfig.rateLimitStringToBytes(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testRateLimitStringToBytes_Empty() {
    // Test empty string throws IllegalArgumentException
    try {
      S3BackendConfig.rateLimitStringToBytes("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
      assertEquals("Cannot convert rate limit string: ", e.getMessage());
    }
  }

  @Test
  public void testInvalidRateLimit() {
    // Test negative rate limit throws IllegalArgumentException
    try {
      new S3BackendConfig(true, -1024, 5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
      assertEquals("rateLimitBytes must be >= 0", e.getMessage());
    }
  }

  @Test
  public void testZeroRateLimit() {
    // Test zero rate limit is valid
    assertEquals(0, S3BackendConfig.rateLimitStringToBytes("0"));
  }

  @Test
  public void testInvalidWindowSeconds() {
    // Test zero or negative window seconds throws IllegalArgumentException
    try {
      new S3BackendConfig(true, 1024, 0);
      fail("Expected IllegalArgumentException for zero window seconds");
    } catch (IllegalArgumentException e) {
      // Expected
      assertEquals("rateLimitWindowSeconds must be > 0", e.getMessage());
    }

    try {
      new S3BackendConfig(true, 1024, -5);
      fail("Expected IllegalArgumentException for negative window seconds");
    } catch (IllegalArgumentException e) {
      // Expected
      assertEquals("rateLimitWindowSeconds must be > 0", e.getMessage());
    }
  }
}
