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
package com.yelp.nrtsearch.server.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class InputStreamDataInputTest {

  @Test
  public void testReadByte() throws IOException {
    byte[] data = {0x01, 0x02, (byte) 0xFF, 0x7F, (byte) 0x80};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x01);
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x02);
      assertThat(dataInput.readByte()).isEqualTo((byte) 0xFF);
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x7F);
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x80);
    }
  }

  @Test
  public void testReadByteFromEmptyStream() throws IOException {
    InputStream inputStream = new ByteArrayInputStream(new byte[0]);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      assertThatThrownBy(() -> dataInput.readByte())
          .isInstanceOf(IOException.class)
          .hasMessage("End of stream reached");
    }
  }

  @Test
  public void testReadByteAtEndOfStream() throws IOException {
    byte[] data = {0x01};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      // Read the only byte
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x01);

      // Next read should throw exception
      assertThatThrownBy(() -> dataInput.readByte())
          .isInstanceOf(IOException.class)
          .hasMessage("End of stream reached");
    }
  }

  @Test
  public void testReadBytes() throws IOException {
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      byte[] buffer = new byte[4];
      dataInput.readBytes(buffer, 0, 4);
      assertThat(buffer).containsExactly(0x01, 0x02, 0x03, 0x04);

      buffer = new byte[4];
      dataInput.readBytes(buffer, 0, 4);
      assertThat(buffer).containsExactly(0x05, 0x06, 0x07, 0x08);
    }
  }

  @Test
  public void testReadBytesWithOffset() throws IOException {
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      byte[] buffer = new byte[10];
      dataInput.readBytes(buffer, 3, 5);

      // Check that only the specified range was filled
      assertThat(buffer[0]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[1]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[2]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[3]).isEqualTo((byte) 0x01); // start of data
      assertThat(buffer[4]).isEqualTo((byte) 0x02);
      assertThat(buffer[5]).isEqualTo((byte) 0x03);
      assertThat(buffer[6]).isEqualTo((byte) 0x04);
      assertThat(buffer[7]).isEqualTo((byte) 0x05); // end of data
      assertThat(buffer[8]).isEqualTo((byte) 0x00); // untouched
      assertThat(buffer[9]).isEqualTo((byte) 0x00); // untouched
    }
  }

  @Test
  public void testReadBytesPartialRead() throws IOException {
    // Create a mock InputStream that returns partial reads
    InputStream mockStream = mock(InputStream.class);
    when(mockStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenReturn(2) // First call returns 2 bytes
        .thenReturn(3) // Second call returns 3 bytes
        .thenReturn(-1); // Third call returns EOF

    try (InputStreamDataInput dataInput = new InputStreamDataInput(mockStream)) {
      byte[] buffer = new byte[10];

      dataInput.readBytes(buffer, 0, 5);

      // Verify that read was called twice to get all 5 bytes
      verify(mockStream, times(2)).read(any(byte[].class), anyInt(), anyInt());
    }
  }

  @Test
  public void testReadBytesEndOfStream() throws IOException {
    byte[] data = {0x01, 0x02, 0x03};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      byte[] buffer = new byte[5];
      assertThatThrownBy(() -> dataInput.readBytes(buffer, 0, 5))
          .isInstanceOf(IOException.class)
          .hasMessage("End of stream reached before reading all bytes");
    }
  }

  @Test
  public void testReadBytesZeroLength() throws IOException {
    byte[] data = {0x01, 0x02, 0x03};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      byte[] buffer = new byte[5];
      // Reading zero bytes should not throw and should not consume any data
      dataInput.readBytes(buffer, 0, 0);

      // Verify we can still read the first byte
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x01);
    }
  }

  @Test
  public void testSkipBytes() throws IOException {
    byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      // Skip 3 bytes
      dataInput.skipBytes(3);

      // Next read should return the 4th byte
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x04);

      // Skip 2 more bytes
      dataInput.skipBytes(2);

      // Next read should return the 7th byte
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x07);
    }
  }

  @Test
  public void testSkipBytesZero() throws IOException {
    byte[] data = {0x01, 0x02, 0x03};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      // Skipping zero bytes should be a no-op
      dataInput.skipBytes(0);

      // Should still be able to read the first byte
      assertThat(dataInput.readByte()).isEqualTo((byte) 0x01);
    }
  }

  @Test
  public void testSkipBytesWithPartialSkips() throws IOException {
    // Create a mock InputStream that returns partial skips
    InputStream mockStream = mock(InputStream.class);
    when(mockStream.skip(anyLong()))
        .thenReturn(2L) // First skip call returns 2
        .thenReturn(3L) // Second skip call returns 3
        .thenReturn(0L); // Third skip call returns 0 (no more skips possible)
    when(mockStream.read())
        .thenReturn(0x01) // When skip returns 0, try to read
        .thenReturn(-1); // Then EOF

    try (InputStreamDataInput dataInput = new InputStreamDataInput(mockStream)) {
      dataInput.skipBytes(6); // Should require multiple skip calls

      verify(mockStream, times(3)).skip(anyLong());
      verify(mockStream, times(1)).read(); // Should call read once when skip returns 0
    }
  }

  @Test
  public void testSkipBytesEndOfStream() throws IOException {
    byte[] data = {0x01, 0x02, 0x03};
    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      assertThatThrownBy(() -> dataInput.skipBytes(5))
          .isInstanceOf(IOException.class)
          .hasMessage("End of stream reached while skipping bytes");
    }
  }

  @Test
  public void testSkipBytesWithReadFallback() throws IOException {
    // Create a mock that can't skip but can read
    InputStream mockStream = mock(InputStream.class);
    when(mockStream.skip(anyLong())).thenReturn(0L); // Always returns 0 (can't skip)
    when(mockStream.read())
        .thenReturn(0x01) // Return some bytes when read
        .thenReturn(0x02)
        .thenReturn(0x03)
        .thenReturn(-1); // Then EOF

    try (InputStreamDataInput dataInput = new InputStreamDataInput(mockStream)) {
      dataInput.skipBytes(3);

      // Should have called read 3 times since skip always returns 0
      verify(mockStream, times(3)).read();
    }
  }

  @Test
  public void testSkipBytesWithReadFallbackEndOfStream() throws IOException {
    // Create a mock that can't skip and hits EOF during read
    InputStream mockStream = mock(InputStream.class);
    when(mockStream.skip(anyLong())).thenReturn(0L); // Always returns 0 (can't skip)
    when(mockStream.read())
        .thenReturn(0x01) // Return one byte
        .thenReturn(-1); // Then EOF

    try (InputStreamDataInput dataInput = new InputStreamDataInput(mockStream)) {
      assertThatThrownBy(() -> dataInput.skipBytes(3))
          .isInstanceOf(IOException.class)
          .hasMessage("End of stream reached while skipping bytes");
    }
  }

  @Test
  public void testClose() throws IOException {
    InputStream mockStream = mock(InputStream.class);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(mockStream)) {
      // The close will be called automatically by try-with-resources
    }
    verify(mockStream, times(1)).close();
  }

  @Test
  public void testCloseWithIOException() throws IOException {
    InputStream mockStream = mock(InputStream.class);
    IOException expectedException = new IOException("Close failed");
    doThrow(expectedException).when(mockStream).close();

    InputStreamDataInput dataInput = new InputStreamDataInput(mockStream);

    assertThatThrownBy(() -> dataInput.close()).isEqualTo(expectedException);
  }

  @Test
  public void testIntegrationWithRealData() throws IOException {
    // Test with a larger dataset to ensure everything works together
    byte[] data = new byte[1000];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }

    InputStream inputStream = new ByteArrayInputStream(data);
    try (InputStreamDataInput dataInput = new InputStreamDataInput(inputStream)) {
      // Read some individual bytes
      assertThat(dataInput.readByte()).isEqualTo((byte) 0);
      assertThat(dataInput.readByte()).isEqualTo((byte) 1);
      assertThat(dataInput.readByte()).isEqualTo((byte) 2);

      // Skip some bytes
      dataInput.skipBytes(10);

      // Read a chunk
      byte[] buffer = new byte[5];
      dataInput.readBytes(buffer, 0, 5);
      assertThat(buffer).containsExactly((byte) 13, (byte) 14, (byte) 15, (byte) 16, (byte) 17);

      // Skip to near the end
      dataInput.skipBytes(975);

      // Read the last few bytes
      assertThat(dataInput.readByte()).isEqualTo((byte) (993 % 256));
      assertThat(dataInput.readByte()).isEqualTo((byte) (994 % 256));
    }
  }
}
