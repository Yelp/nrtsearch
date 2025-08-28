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
package org.apache.lucene.replicator.nrt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class StreamCopyOneFileTest {
  private static final int CHUNK_SIZE = 1024;
  private static final Random rand = new Random();

  @Test
  public void testCopySmallFile() throws IOException {
    copyAndVerify(100);
  }

  @Test
  public void testCopyFullChunk() throws IOException {
    copyAndVerify(CHUNK_SIZE);
  }

  @Test
  public void testCopyMultiChunks() throws IOException {
    copyAndVerify(CHUNK_SIZE + 100);
  }

  @Test
  public void testCopyFileWithChecksumOnly() throws IOException {
    copyAndVerify(Long.BYTES);
  }

  @Test
  public void testCopyFileWithMinimalData() throws IOException {
    copyAndVerify(Long.BYTES + 1);
  }

  @Test
  public void testChecksumsMatch() throws IOException {
    long checksum = rand.nextLong();
    int fileLength = 100;

    MockDataInput mockDataInput = createMockDataInput(fileLength, checksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(checksum);
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData = new FileMetaData(new byte[0], new byte[0], fileLength, checksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    try (StreamCopyOneFile copyOneFile =
        new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer)) {

      // Copy all data
      boolean done = false;
      while (!done) {
        done = copyOneFile.visit();
      }

      // Verify the file was copied correctly
      verify(mockNode).startCopyFile("test_file");
      verify(mockNode).finishCopyFile("test_file");
      verify(mockOutput).close();
    }
  }

  @Test
  public void testChecksumMismatchThrowsException() throws IOException {
    long correctChecksum = rand.nextLong();
    long wrongChecksum = correctChecksum + 1; // Different checksum
    int fileLength = 100;

    MockDataInput mockDataInput = createMockDataInput(fileLength, correctChecksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(wrongChecksum); // Wrong checksum
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData =
        new FileMetaData(new byte[0], new byte[0], fileLength, correctChecksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    try (StreamCopyOneFile copyOneFile =
        new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer)) {

      // Copy all data - should throw on final visit
      assertThrows(
          IOException.class,
          () -> {
            boolean done = false;
            while (!done) {
              done = copyOneFile.visit();
            }
          });
    }
  }

  @Test
  public void testRemoteChecksumMismatchThrowsException() throws IOException {
    long correctChecksum = rand.nextLong();
    long wrongRemoteChecksum = correctChecksum + 1;
    int fileLength = 100;

    MockDataInput mockDataInput = createMockDataInput(fileLength, wrongRemoteChecksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(correctChecksum);
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData =
        new FileMetaData(new byte[0], new byte[0], fileLength, correctChecksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    // Copy all data - should throw on final visit due to remote checksum mismatch
    assertThrows(
        IOException.class,
        () -> {
          try (StreamCopyOneFile copyOneFile =
              new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer)) {
            boolean done = false;
            while (!done) {
              done = copyOneFile.visit();
            }
          }
        });
  }

  @Test
  public void testGetters() throws IOException {
    long checksum = rand.nextLong();
    int fileLength = 100;

    MockDataInput mockDataInput = createMockDataInput(fileLength, checksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(checksum);
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData = new FileMetaData(new byte[0], new byte[0], fileLength, checksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    try (StreamCopyOneFile copyOneFile =
        new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer)) {

      // Test getters
      assertEquals("test_file", copyOneFile.getFileName());
      assertEquals("test_file.tmp", copyOneFile.getFileTmpName());
      assertEquals(fileMetaData, copyOneFile.getFileMetaData());
      assertEquals(fileLength - Long.BYTES, copyOneFile.getBytesToCopy());
      assertEquals(0, copyOneFile.getBytesCopied()); // Initially 0

      // Copy some data and check bytes copied
      copyOneFile.visit();
      assertTrue(copyOneFile.getBytesCopied() > 0);
    }
  }

  @Test
  public void testCloseWithCloseableDataInput() throws IOException {
    long checksum = rand.nextLong();
    int fileLength = 100;

    MockCloseableDataInput mockDataInput = new MockCloseableDataInput(fileLength, checksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(checksum);
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData = new FileMetaData(new byte[0], new byte[0], fileLength, checksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    StreamCopyOneFile copyOneFile =
        new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer);

    copyOneFile.close();

    // Verify that the closeable data input was closed
    assertTrue(mockDataInput.isClosed());
    verify(mockOutput).close();
    verify(mockNode).finishCopyFile("test_file");
  }

  private void copyAndVerify(int length) throws IOException {
    long checksum = rand.nextLong();

    MockDataInput mockDataInput = createMockDataInput(length, checksum);
    IndexOutput mockOutput = mock(IndexOutput.class);
    ReplicaNode mockNode = mock(ReplicaNode.class);

    when(mockOutput.getChecksum()).thenReturn(checksum);
    when(mockOutput.getName()).thenReturn("test_file.tmp");
    when(mockNode.createTempOutput(eq("test_file"), eq("copy"), any(IOContext.class)))
        .thenReturn(mockOutput);

    FileMetaData fileMetaData = new FileMetaData(new byte[0], new byte[0], length, checksum);
    byte[] buffer = new byte[CHUNK_SIZE];

    try (StreamCopyOneFile copyOneFile =
        new StreamCopyOneFile(mockDataInput, mockNode, "test_file", fileMetaData, buffer)) {

      // Verify initial state
      assertEquals(0, copyOneFile.getBytesCopied());
      assertEquals(length - Long.BYTES, copyOneFile.getBytesToCopy());

      // Copy the file
      boolean done = false;
      int iterations = 0;
      while (!done && iterations < 1000) { // Safety guard against infinite loops
        done = copyOneFile.visit();
        iterations++;
      }

      assertTrue("File copy should complete", done);

      // Verify final state
      assertEquals(length - Long.BYTES, copyOneFile.getBytesCopied());

      // Verify mocks were called appropriately
      verify(mockNode).startCopyFile("test_file");
      verify(mockNode).finishCopyFile("test_file");
      verify(mockOutput).close();

      // Verify data was written
      if (length > Long.BYTES) {
        // Calculate expected number of writeBytes calls based on chunk size
        int dataLength = length - (int) Long.BYTES;
        int expectedCalls = (dataLength + CHUNK_SIZE - 1) / CHUNK_SIZE; // Ceiling division
        verify(mockOutput, times(expectedCalls))
            .writeBytes(any(byte[].class), eq(0), any(Integer.class));
      }
    }
  }

  private MockDataInput createMockDataInput(int totalLength, long checksum) {
    int dataLength = totalLength - Long.BYTES;
    byte[] data = new byte[dataLength];
    rand.nextBytes(data);

    return new MockDataInput(data, checksum);
  }

  // Mock DataInput implementation for testing
  private static class MockDataInput extends DataInput {
    private final byte[] data;
    private final long checksum;
    private int position = 0;

    public MockDataInput(byte[] data, long checksum) {
      this.data = data;
      this.checksum = checksum;
    }

    @Override
    public byte readByte() throws IOException {
      if (position < data.length) {
        return data[position++];
      } else if (position < data.length + Long.BYTES) {
        // Return checksum bytes
        int checksumByteIndex = position - data.length;
        position++;
        return (byte) (checksum >>> (8 * (Long.BYTES - 1 - checksumByteIndex)));
      } else {
        throw new IOException("End of input");
      }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      for (int i = 0; i < len; i++) {
        b[offset + i] = readByte();
      }
    }

    @Override
    public void skipBytes(long count) throws IOException {
      for (long i = 0; i < count; i++) {
        readByte(); // Simple implementation for testing
      }
    }
  }

  // Mock Closeable DataInput for testing close functionality
  private static class MockCloseableDataInput extends MockDataInput implements Closeable {
    private boolean closed = false;

    public MockCloseableDataInput(byte[] data, long checksum) {
      super(data, checksum);
    }

    public MockCloseableDataInput(int totalLength, long checksum) {
      super(new byte[totalLength - Long.BYTES], checksum);
      rand.nextBytes(super.data);
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
