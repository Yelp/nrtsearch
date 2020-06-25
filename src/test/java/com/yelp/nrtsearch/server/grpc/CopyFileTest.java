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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyFileTest {
  private static final int TOTAL_CHUNKS = 10;
  private static final int CHUNK_SIZE = 1024 * 64;
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void sendRawFile() throws Exception {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new LuceneServer.ReplicationServerImpl(null))
            .build()
            .start());

    ReplicationServerGrpc.ReplicationServerBlockingStub blockingStub =
        ReplicationServerGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    ReplicationServerGrpc.ReplicationServerStub stub =
        ReplicationServerGrpc.newStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    CountDownLatch finishLatch = new CountDownLatch(1);

    StreamObserver<TransferStatus> responseObserver = new SendRawFileStreamObserver(finishLatch);
    StreamObserver<RawFileChunk> requestObserver = stub.sendRawFile(responseObserver);
    Random random = new Random();
    byte[] chunk = new byte[CHUNK_SIZE];
    for (int i = 0; i < TOTAL_CHUNKS; i++) {
      random.nextBytes(chunk);
      requestObserver.onNext(
          RawFileChunk.newBuilder().setContent(ByteString.copyFrom(chunk)).build());
    }
    // Mark the end of requests
    requestObserver.onCompleted();
    // Receiving happens asynchronously, so block here 20 seconds
    if (!finishLatch.await(20, TimeUnit.SECONDS)) {
      throw new RuntimeException("sendRaw can not finish within 20 seconds");
    }

    assertEquals(true, ((SendRawFileStreamObserver) responseObserver).isCompleted());
    assertEquals(false, ((SendRawFileStreamObserver) responseObserver).isError());
    assertEquals(
        true,
        ((SendRawFileStreamObserver) responseObserver)
            .getTransferStatus()
            .getCode()
            .equals(TransferStatusCode.Done));

    System.out.println(
        ((SendRawFileStreamObserver) responseObserver).getTransferStatus().getMessage());
  }

  //    @Test
  //    public void recvRawFileOnBlockingClient() throws Exception {
  //        // Generate a unique in-process server name.
  //        String serverName = InProcessServerBuilder.generateName();
  //
  //        // Create a server, add service, start, and register for automatic graceful shutdown.
  //        grpcCleanup.register(InProcessServerBuilder
  //                .forName(serverName).directExecutor().addService(new
  // LuceneServer.ReplicationServerImpl(null)).build().start());
  //
  //        ReplicationServerGrpc.ReplicationServerBlockingStub blockingStub =
  // ReplicationServerGrpc.newBlockingStub(
  //                // Create a client channel and register for automatic graceful shutdown.
  //
  // grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  //        ReplicationServerGrpc.ReplicationServerStub stub = ReplicationServerGrpc.newStub(
  //
  // grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  //
  //        //create and write to file total_chunk * chunk_size
  //        File tempFile = folder.newFile("temp");
  //        OutputStream outputStream = new FileOutputStream(tempFile);
  //        Random random = new Random();
  //        byte[] chunk = new byte[CHUNK_SIZE];
  //        for (int i = 0; i < TOTAL_CHUNKS; i++) {
  //            random.nextBytes(chunk);
  //            outputStream.write(chunk);
  //        }
  //        outputStream.close();
  //
  //        Iterator<RawFileChunk> rawFileChunks =
  // blockingStub.recvRawFile(FileInfo.newBuilder().setFileName(tempFile.getAbsolutePath()).setFpStart(0).build());
  //        long totalBytesReceived = 0;
  //        while(rawFileChunks.hasNext()){
  //            RawFileChunk rawFileChunk = rawFileChunks.next();
  //            totalBytesReceived+=rawFileChunk.getContent().size();
  //        }
  //        assertEquals(CHUNK_SIZE * TOTAL_CHUNKS, totalBytesReceived);
  //    }

  static class SendRawFileStreamObserver implements StreamObserver<TransferStatus> {

    private final CountDownLatch finishLatch;
    private TransferStatus transferStatus;
    private boolean error;
    private boolean completed;

    SendRawFileStreamObserver(CountDownLatch finishLatch) {
      this.finishLatch = finishLatch;
      this.transferStatus = null;
    }

    @Override
    public void onNext(TransferStatus value) {
      transferStatus = value;
    }

    @Override
    public void onError(Throwable t) {
      error = true;
      finishLatch.countDown();
    }

    @Override
    public void onCompleted() {
      completed = true;
      finishLatch.countDown();
    }

    public boolean isError() {
      return error;
    }

    public boolean isCompleted() {
      return completed;
    }

    public TransferStatus getTransferStatus() {
      return transferStatus;
    }
  }
}
