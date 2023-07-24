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
package org.apache.lucene.replicator.nrt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CopyOneFileTest {
  private static final int CHUNK_SIZE = 1024 * 10;
  private static final Random rand = new Random();

  @Test
  public void testCopyFile() throws IOException {
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
  public void testChecksumInSeparateChunk() throws IOException {
    copyAndVerify(CHUNK_SIZE + Long.BYTES);
  }

  @Test
  public void testSplitChecksum() throws IOException {
    for (int i = 1; i < Long.BYTES; ++i) {
      copyAndVerify(CHUNK_SIZE + i);
    }
  }

  @Test
  public void testCopyNoData() throws IOException {
    copyAndVerify(Long.BYTES);
  }

  private void copyAndVerify(int length) throws IOException {
    long checksum = rand.nextLong();

    IndexOutput mockOutput = mock(IndexOutput.class);
    when(mockOutput.getChecksum()).thenReturn(checksum);

    ByteBuffer fileBuffer = getFileBuffer(length, checksum);
    List<RawFileChunk> chunks = getChunks(fileBuffer);

    ReplicaNode mockNode = mock(ReplicaNode.class);
    when(mockNode.createTempOutput("test_copy", "copy", IOContext.DEFAULT)).thenReturn(mockOutput);
    // copy does not use header or footer
    FileMetaData fileMetaData = new FileMetaData(new byte[0], new byte[0], length, checksum);
    CopyOneFile copyOneFile =
        new CopyOneFile(chunks.listIterator(), mockNode, "test_copy", fileMetaData);

    for (int i = 0; i < chunks.size(); ++i) {
      assertFalse(copyOneFile.visit());
    }
    assertTrue(copyOneFile.visit());

    verify(mockOutput).close();

    ByteBuffer copiedBuffer = ByteBuffer.allocate(length);
    if (length > Long.BYTES) {
      ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
      ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);

      int dataLength = length - Long.BYTES;
      int expectedCalls = dataLength / CHUNK_SIZE;
      if ((dataLength % CHUNK_SIZE) > 0) {
        expectedCalls++;
      }
      verify(mockOutput, times(expectedCalls))
          .writeBytes(bytesCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture());

      List<byte[]> argBytes = bytesCaptor.getAllValues();
      List<Integer> argOffset = offsetCaptor.getAllValues();
      List<Integer> argLength = lengthCaptor.getAllValues();

      int totalSize = 0;
      for (int i = 0; i < argBytes.size(); ++i) {
        assertEquals(Integer.valueOf(0), argOffset.get(i));
        assertTrue(argLength.get(i) > 0);
        copiedBuffer.put(argBytes.get(i), argOffset.get(i), argLength.get(i));
        totalSize += argLength.get(i);
      }
      assertEquals(length - Long.BYTES, totalSize);
    }

    ArgumentCaptor<Byte> checksumCaptor = ArgumentCaptor.forClass(byte.class);
    verify(mockOutput, times(8)).writeByte(checksumCaptor.capture());

    for (Byte b : checksumCaptor.getAllValues()) {
      copiedBuffer.put(b);
    }

    fileBuffer.rewind();
    copiedBuffer.rewind();
    assertEquals(fileBuffer, copiedBuffer);
  }

  private ByteBuffer getFileBuffer(int length, long checksum) {
    int dataLength = length - Long.BYTES;
    byte[] data = new byte[dataLength];
    rand.nextBytes(data);
    ByteBuffer fileBuffer = ByteBuffer.allocate(length);
    fileBuffer.put(data);
    fileBuffer.putLong(checksum);
    return fileBuffer.rewind();
  }

  private List<RawFileChunk> getChunks(ByteBuffer fileBuffer) {
    List<RawFileChunk> chunks = new ArrayList<>();
    while (fileBuffer.remaining() > 0) {
      int size = Math.min(fileBuffer.remaining(), CHUNK_SIZE);
      chunks.add(
          RawFileChunk.newBuilder().setContent(ByteString.copyFrom(fileBuffer, size)).build());
    }
    return chunks;
  }
}
