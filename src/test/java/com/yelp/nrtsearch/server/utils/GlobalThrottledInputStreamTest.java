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
package com.yelp.nrtsearch.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class GlobalThrottledInputStreamTest {

  @Test
  public void testBasicReadFunctionality() throws IOException {
    // Arrange
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
    InputStream inputStream = new ByteArrayInputStream(data);
    GlobalWindowRateLimiter mockLimiter = mock(GlobalWindowRateLimiter.class);

    // Act
    try (GlobalThrottledInputStream throttledStream =
        new GlobalThrottledInputStream(inputStream, mockLimiter)) {
      // Assert
      assertThat(throttledStream.read()).isEqualTo(0x01);
      assertThat(throttledStream.read()).isEqualTo(0x02);
      assertThat(throttledStream.read()).isEqualTo(0x03);
      assertThat(throttledStream.read()).isEqualTo(0x04);
      assertThat(throttledStream.read()).isEqualTo(0x05);
      assertThat(throttledStream.read()).isEqualTo(-1); // End of stream
    }

    // Verify limiter was called for each byte read
    verify(mockLimiter, times(5)).acquire(1);
    verifyNoMoreInteractions(mockLimiter);
  }

  @Test
  public void testReadByteArray() throws IOException {
    // Arrange
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
    InputStream inputStream = new ByteArrayInputStream(data);
    GlobalWindowRateLimiter mockLimiter = mock(GlobalWindowRateLimiter.class);

    // Act
    try (GlobalThrottledInputStream throttledStream =
        new GlobalThrottledInputStream(inputStream, mockLimiter)) {
      byte[] buffer = new byte[5];
      int bytesRead = throttledStream.read(buffer);

      // Assert
      assertThat(bytesRead).isEqualTo(5);
      assertThat(buffer).containsExactly(0x01, 0x02, 0x03, 0x04, 0x05);

      // Verify end of stream
      assertThat(throttledStream.read()).isEqualTo(-1);
    }

    // Verify limiter was called once with the total bytes read
    verify(mockLimiter, times(1)).acquire(5);
    verifyNoMoreInteractions(mockLimiter);
  }

  @Test
  public void testReadByteArrayWithOffsetAndLength() throws IOException {
    // Arrange
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
    InputStream inputStream = new ByteArrayInputStream(data);
    GlobalWindowRateLimiter mockLimiter = mock(GlobalWindowRateLimiter.class);

    // Act
    try (GlobalThrottledInputStream throttledStream =
        new GlobalThrottledInputStream(inputStream, mockLimiter)) {
      byte[] buffer = new byte[10]; // Larger buffer than needed
      int bytesRead = throttledStream.read(buffer, 2, 3); // Read 3 bytes starting at offset 2

      // Assert
      assertThat(bytesRead).isEqualTo(3);

      // Check that only the specified range was filled
      assertThat(buffer[0]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[1]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[2]).isEqualTo((byte) 0x01); // start of data
      assertThat(buffer[3]).isEqualTo((byte) 0x02);
      assertThat(buffer[4]).isEqualTo((byte) 0x03); // end of data
      assertThat(buffer[5]).isEqualTo((byte) 0x00); // untouched

      // Read the remaining bytes
      bytesRead = throttledStream.read(buffer, 5, 2);
      assertThat(bytesRead).isEqualTo(2);
      assertThat(buffer[5]).isEqualTo((byte) 0x04);
      assertThat(buffer[6]).isEqualTo((byte) 0x05);

      // Verify end of stream
      assertThat(throttledStream.read()).isEqualTo(-1);
    }

    // Verify limiter was called twice with the correct number of bytes
    verify(mockLimiter, times(1)).acquire(3);
    verify(mockLimiter, times(1)).acquire(2);
    verifyNoMoreInteractions(mockLimiter);
  }

  @Test
  public void testEmptyStream() throws IOException {
    // Arrange
    byte[] data = new byte[0];
    InputStream inputStream = new ByteArrayInputStream(data);
    GlobalWindowRateLimiter mockLimiter = mock(GlobalWindowRateLimiter.class);

    // Act
    try (GlobalThrottledInputStream throttledStream =
        new GlobalThrottledInputStream(inputStream, mockLimiter)) {
      // Assert
      assertThat(throttledStream.read()).isEqualTo(-1); // End of stream

      // Try to read into a buffer
      byte[] buffer = new byte[5];
      assertThat(throttledStream.read(buffer)).isEqualTo(-1);

      // Try to read into a buffer with offset and length
      assertThat(throttledStream.read(buffer, 1, 3)).isEqualTo(-1);
    }

    // Verify limiter was never called since no bytes were read
    verifyNoMoreInteractions(mockLimiter);
  }

  @Test
  public void testPartialRead() throws IOException {
    // Arrange - create a stream that will return fewer bytes than requested
    byte[] data = {0x01, 0x02, 0x03};
    InputStream inputStream = new ByteArrayInputStream(data);
    GlobalWindowRateLimiter mockLimiter = mock(GlobalWindowRateLimiter.class);

    // Act
    try (GlobalThrottledInputStream throttledStream =
        new GlobalThrottledInputStream(inputStream, mockLimiter)) {
      byte[] buffer = new byte[5];
      int bytesRead = throttledStream.read(buffer);

      // Assert
      assertThat(bytesRead).isEqualTo(3); // Only 3 bytes available
      assertThat(buffer[0]).isEqualTo((byte) 0x01);
      assertThat(buffer[1]).isEqualTo((byte) 0x02);
      assertThat(buffer[2]).isEqualTo((byte) 0x03);

      // Next read should return EOF
      assertThat(throttledStream.read()).isEqualTo(-1);
    }

    // Verify limiter was called with the actual number of bytes read
    verify(mockLimiter, times(1)).acquire(3);
    verifyNoMoreInteractions(mockLimiter);
  }
}
