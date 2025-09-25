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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.monitoring.S3DownloadStreamWrapper;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class S3BackendWrapDownloadStreamTest {

  private static final String TEST_INDEX_IDENTIFIER = "test_index_123";
  private static final String TEST_BASE_INDEX_NAME = "test_index";
  private static final byte[] TEST_DATA = "test data content".getBytes();

  @Test
  public void testWrapDownloadStream_withMetricsEnabled() throws IOException {
    // Arrange
    InputStream inputStream = new ByteArrayInputStream(TEST_DATA);
    boolean s3Metrics = true;

    // Mock the static method call to BackendGlobalState.getBaseIndexName
    try (MockedStatic<BackendGlobalState> mockedStatic =
        Mockito.mockStatic(BackendGlobalState.class)) {
      mockedStatic
          .when(() -> BackendGlobalState.getBaseIndexName(TEST_INDEX_IDENTIFIER))
          .thenReturn(TEST_BASE_INDEX_NAME);

      // Act
      InputStream result =
          S3Backend.wrapDownloadStream(inputStream, s3Metrics, TEST_INDEX_IDENTIFIER);

      // Assert
      assertTrue(
          "Result should be an instance of S3DownloadStreamWrapper",
          result instanceof S3DownloadStreamWrapper);

      // Verify the content can still be read correctly
      byte[] buffer = new byte[TEST_DATA.length];
      int bytesRead = result.read(buffer);
      assertEquals(TEST_DATA.length, bytesRead);
      assertEquals(new String(TEST_DATA), new String(buffer));

      // Verify the static method was called with the correct parameter
      mockedStatic.verify(() -> BackendGlobalState.getBaseIndexName(TEST_INDEX_IDENTIFIER));
    }
  }

  @Test
  public void testWrapDownloadStream_withMetricsDisabled() throws IOException {
    // Arrange
    InputStream inputStream = new ByteArrayInputStream(TEST_DATA);
    boolean s3Metrics = false;

    // Act
    InputStream result =
        S3Backend.wrapDownloadStream(inputStream, s3Metrics, TEST_INDEX_IDENTIFIER);

    // Assert
    assertSame(
        "Result should be the same instance as the input stream when metrics are disabled",
        inputStream,
        result);

    // Verify the content can still be read correctly
    byte[] buffer = new byte[TEST_DATA.length];
    int bytesRead = result.read(buffer);
    assertEquals(TEST_DATA.length, bytesRead);
    assertEquals(new String(TEST_DATA), new String(buffer));
  }

  @Test
  public void testWrapDownloadStream_withNullIndexIdentifier() throws IOException {
    // Arrange
    InputStream inputStream = new ByteArrayInputStream(TEST_DATA);
    boolean s3Metrics = true;
    String nullIndexIdentifier = null;

    // Mock the static method call to BackendGlobalState.getBaseIndexName
    try (MockedStatic<BackendGlobalState> mockedStatic =
        Mockito.mockStatic(BackendGlobalState.class)) {
      mockedStatic
          .when(() -> BackendGlobalState.getBaseIndexName(nullIndexIdentifier))
          .thenReturn(null);

      // Act
      InputStream result =
          S3Backend.wrapDownloadStream(inputStream, s3Metrics, nullIndexIdentifier);

      // Assert
      assertTrue(
          "Result should be an instance of S3DownloadStreamWrapper even with null index",
          result instanceof S3DownloadStreamWrapper);

      // Verify the content can still be read correctly
      byte[] buffer = new byte[TEST_DATA.length];
      int bytesRead = result.read(buffer);
      assertEquals(TEST_DATA.length, bytesRead);
      assertEquals(new String(TEST_DATA), new String(buffer));

      // Verify the static method was called with null
      mockedStatic.verify(() -> BackendGlobalState.getBaseIndexName(nullIndexIdentifier));
    }
  }

  @Test
  public void testWrapDownloadStream_withEmptyStream() throws IOException {
    // Arrange
    InputStream emptyStream = new ByteArrayInputStream(new byte[0]);
    boolean s3Metrics = true;

    // Mock the static method call to BackendGlobalState.getBaseIndexName
    try (MockedStatic<BackendGlobalState> mockedStatic =
        Mockito.mockStatic(BackendGlobalState.class)) {
      mockedStatic
          .when(() -> BackendGlobalState.getBaseIndexName(TEST_INDEX_IDENTIFIER))
          .thenReturn(TEST_BASE_INDEX_NAME);

      // Act
      InputStream result =
          S3Backend.wrapDownloadStream(emptyStream, s3Metrics, TEST_INDEX_IDENTIFIER);

      // Assert
      assertTrue(
          "Result should be an instance of S3DownloadStreamWrapper",
          result instanceof S3DownloadStreamWrapper);

      // Verify the stream is empty
      assertEquals(-1, result.read());

      // Verify the static method was called with the correct parameter
      mockedStatic.verify(() -> BackendGlobalState.getBaseIndexName(TEST_INDEX_IDENTIFIER));
    }
  }
}
