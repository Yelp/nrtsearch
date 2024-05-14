/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.FileInfo;
import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import com.yelp.nrtsearch.server.luceneserver.SimpleCopyJob.FileChunkStreamingIterator;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SimpleCopyJobTest {

  @Test
  public void testAckedCopyNoAcks() {
    doAckedTest(100, 10, 100);
  }

  @Test
  public void testAckedCopy() {
    doAckedTest(10, 1000, 1000);
    doAckedTest(10, 1000, 100);
    doAckedTest(10, 1000, 10);
    doAckedTest(10, 1000, 1);
  }

  @Test
  public void testOnError() {
    @SuppressWarnings("unchecked")
    StreamObserver<FileInfo> mockObserver = (StreamObserver<FileInfo>) mock(StreamObserver.class);
    ArgumentCaptor<FileInfo> inputCapture = ArgumentCaptor.forClass(FileInfo.class);
    doNothing().when(mockObserver).onNext(inputCapture.capture());

    Throwable expectedError = new Throwable();

    FileChunkStreamingIterator fcsi = new FileChunkStreamingIterator("test_index");
    fcsi.init(mockObserver);
    sendData(fcsi, 10, 5, 1);
    for (int i = 0; i < 5; ++i) {
      fcsi.next();
    }
    fcsi.onError(expectedError);
    try {
      fcsi.hasNext();
      fail();
    } catch (RuntimeException e) {
      assertSame(expectedError, e.getCause());
    }

    verify(mockObserver, times(1)).onError(any(Throwable.class));
    verify(mockObserver, times(5)).onNext(any(FileInfo.class));
    verifyNoMoreInteractions(mockObserver);
  }

  @Test
  public void testSkipAcksOnComplete() {
    doSkippedAckedTest(10, 1000, 1000);
    doSkippedAckedTest(10, 1000, 100);
    doSkippedAckedTest(10, 1000, 10);
    doSkippedAckedTest(10, 1000, 1);
  }

  private void doAckedTest(int chunkSize, int numChunks, int ackEvery) {
    @SuppressWarnings("unchecked")
    StreamObserver<FileInfo> mockObserver = (StreamObserver<FileInfo>) mock(StreamObserver.class);
    ArgumentCaptor<FileInfo> inputCapture = ArgumentCaptor.forClass(FileInfo.class);
    doNothing().when(mockObserver).onNext(inputCapture.capture());

    FileChunkStreamingIterator fcsi = new FileChunkStreamingIterator("test_index");
    fcsi.init(mockObserver);
    sendData(fcsi, chunkSize, numChunks, ackEvery);
    verifyData(fcsi, chunkSize, numChunks, ackEvery);
    fcsi.onCompleted();
    assertFalse(fcsi.hasNext());
    verifyParams(inputCapture.getAllValues(), numChunks, ackEvery);

    verify(mockObserver, times(1)).onCompleted();
    verify(mockObserver, times(numChunks / ackEvery)).onNext(any(FileInfo.class));
    verifyNoMoreInteractions(mockObserver);
  }

  private void doSkippedAckedTest(int chunkSize, int numChunks, int ackEvery) {
    @SuppressWarnings("unchecked")
    StreamObserver<FileInfo> mockObserver = (StreamObserver<FileInfo>) mock(StreamObserver.class);
    ArgumentCaptor<FileInfo> inputCapture = ArgumentCaptor.forClass(FileInfo.class);
    doNothing().when(mockObserver).onNext(inputCapture.capture());

    FileChunkStreamingIterator fcsi = new FileChunkStreamingIterator("test_index");
    fcsi.init(mockObserver);
    sendData(fcsi, chunkSize, numChunks, ackEvery);
    fcsi.onCompleted();
    verifyData(fcsi, chunkSize, numChunks, ackEvery);
    assertFalse(fcsi.hasNext());
    assertEquals(0, inputCapture.getAllValues().size());

    verify(mockObserver, times(1)).onCompleted();
    verifyNoMoreInteractions(mockObserver);
  }

  private void sendData(
      FileChunkStreamingIterator fcsi, int chunkSize, int numChunks, int ackEvery) {
    byte[] buffer = new byte[chunkSize];
    for (int i = 1; i <= numChunks; ++i) {
      Arrays.fill(buffer, (byte) i);
      RawFileChunk chunk =
          RawFileChunk.newBuilder()
              .setContent(ByteString.copyFrom(buffer))
              .setSeqNum(i)
              .setAck((i % ackEvery) == 0)
              .build();
      fcsi.onNext(chunk);
    }
  }

  private void verifyData(
      FileChunkStreamingIterator fcsi, int chunkSize, int numChunks, int ackEvery) {
    byte[] buffer = new byte[chunkSize];
    for (int seq = 1; seq <= numChunks; ++seq) {
      assertTrue(fcsi.hasNext());
      RawFileChunk chunk = fcsi.next();
      assertEquals(seq, chunk.getSeqNum());
      assertEquals((seq % ackEvery) == 0, chunk.getAck());
      ByteString byteString = chunk.getContent();
      assertEquals(chunkSize, byteString.size());
      byteString.copyTo(buffer, 0);
      byte expectedByte = (byte) seq;
      for (byte b : buffer) {
        assertEquals(expectedByte, b);
      }
    }
  }

  private void verifyParams(List<FileInfo> params, int numChunks, int ackEvery) {
    int expectedAcks = numChunks / ackEvery;
    assertEquals(expectedAcks, params.size());
    for (int i = 0; i < expectedAcks; ++i) {
      assertEquals((i + 1) * ackEvery, params.get(i).getAckSeqNum());
    }
  }
}
