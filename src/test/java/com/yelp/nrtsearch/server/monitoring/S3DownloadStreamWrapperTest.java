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
package com.yelp.nrtsearch.server.monitoring;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;

public class S3DownloadStreamWrapperTest {

  private PrometheusRegistry prometheusRegistry;
  private static final String TEST_INDEX = "test_index";

  @Before
  public void setUp() {
    // Create a new registry for each test to avoid interference between tests
    prometheusRegistry = new PrometheusRegistry();

    // Register the counter with the registry
    S3DownloadStreamWrapper.register(prometheusRegistry);

    // Reset the counter for the test index to ensure tests start with a clean state
    // This is necessary because the counter is static and shared across test methods
    S3DownloadStreamWrapper.nrtS3DownloadBytes.clear();
  }

  @Test
  public void testConstructorInitialization() {
    // Create a mock InputStream
    InputStream mockStream = mock(InputStream.class);
    // Create the wrapper
    S3DownloadStreamWrapper wrapper = new S3DownloadStreamWrapper(mockStream, TEST_INDEX);

    // Verify the counter was initialized with the correct index name
    // We can't directly test the private counterDataPoint field, but we can verify
    // that the counter exists with the correct label value by checking its value
    assertEquals(
        0.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);
  }

  @Test
  public void testAfterReadIncrementsCounter() throws IOException {
    // Create test data
    byte[] testData = new byte[100];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) i;
    }

    ByteArrayInputStream inputStream = new ByteArrayInputStream(testData);

    // Create the wrapper
    S3DownloadStreamWrapper wrapper = new S3DownloadStreamWrapper(inputStream, TEST_INDEX);

    // Initial counter value should be 0
    assertEquals(
        0.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);

    // Read 50 bytes
    byte[] buffer = new byte[50];
    int bytesRead = wrapper.read(buffer);

    // Verify bytes read
    assertEquals(50, bytesRead);

    // Verify counter was incremented by 50
    assertEquals(
        50.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);

    // Read 30 more bytes
    bytesRead = wrapper.read(buffer, 0, 30);

    // Verify bytes read
    assertEquals(30, bytesRead);

    // Verify counter was incremented by 30 more (total 80)
    assertEquals(
        80.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);

    // Read a single byte
    int singleByte = wrapper.read();

    // Verify byte read
    assertEquals(80, singleByte);

    // Verify counter was incremented by 1 more (total 81)
    assertEquals(
        81.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);
  }

  @Test
  public void testRegisterWithPrometheusRegistry() {
    // We're already registering the counter in setUp()

    // Verify the counter is registered by checking if we can access it
    // We'll create a wrapper and use it to increment the counter
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[10]);

    S3DownloadStreamWrapper wrapper = new S3DownloadStreamWrapper(inputStream, TEST_INDEX);

    try {
      // Read some bytes to increment the counter
      wrapper.read();

      // Verify the counter was incremented
      assertEquals(
          1.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read from wrapper", e);
    }
  }

  @Test
  public void testAfterReadWithEOF() throws IOException {
    // Create an empty input stream to simulate EOF
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);

    // Create the wrapper
    S3DownloadStreamWrapper wrapper = new S3DownloadStreamWrapper(inputStream, TEST_INDEX);

    // Initial counter value should be 0
    assertEquals(
        0.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);

    // Read from the empty stream (should return -1 for EOF)
    int result = wrapper.read();

    // Verify EOF was returned
    assertEquals(-1, result);

    // Verify counter was not incremented (still 0)
    assertEquals(
        0.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);

    // Try to read into a buffer (should return -1 for EOF)
    byte[] buffer = new byte[10];
    result = wrapper.read(buffer);

    // Verify EOF was returned
    assertEquals(-1, result);

    // Verify counter was not incremented (still 0)
    assertEquals(
        0.0, S3DownloadStreamWrapper.nrtS3DownloadBytes.labelValues(TEST_INDEX).get(), 0.001);
  }
}
