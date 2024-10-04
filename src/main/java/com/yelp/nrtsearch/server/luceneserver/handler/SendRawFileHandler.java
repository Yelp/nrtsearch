/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.luceneserver.state.GlobalState;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendRawFileHandler extends Handler<RawFileChunk, TransferStatus> {
  public SendRawFileHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public StreamObserver<RawFileChunk> handle(StreamObserver<TransferStatus> responseObserver) {
    OutputStream outputStream = null;
    try {
      // TODO: where do we write these files to?
      outputStream = new FileOutputStream(File.createTempFile("tempfile", ".tmp"));
    } catch (IOException e) {
      responseObserver.onError(e);
    }
    return new SendRawFileStreamObserver(outputStream, responseObserver);
  }

  @Override
  public void handle(RawFileChunk rawFileChunk, StreamObserver<TransferStatus> responseObserver) {}

  static class SendRawFileStreamObserver implements StreamObserver<RawFileChunk> {
    private static final Logger logger =
        LoggerFactory.getLogger(SendRawFileStreamObserver.class.getName());
    private final OutputStream outputStream;
    private final StreamObserver<TransferStatus> responseObserver;
    private final long startTime;

    SendRawFileStreamObserver(
        OutputStream outputStream, StreamObserver<TransferStatus> responseObserver) {
      this.outputStream = outputStream;
      this.responseObserver = responseObserver;
      startTime = System.nanoTime();
    }

    @Override
    public void onNext(RawFileChunk value) {
      // called by client once per chunk of data
      try {
        logger.trace("sendRawFile onNext");
        value.getContent().writeTo(outputStream);
      } catch (IOException e) {
        try {
          outputStream.close();
        } catch (IOException ex) {
          logger.warn("error trying to close outputStream", ex);
        } finally {
          // we either had error in writing to outputStream or cant close it,
          // either case we need to raise it back to client
          responseObserver.onError(e);
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      logger.warn("sendRawFile cancelled", t);
      try {
        outputStream.close();
      } catch (IOException e) {
        logger.warn("error while trying to close outputStream", e);
      } finally {
        // we want to raise error always here
        responseObserver.onError(t);
      }
    }

    @Override
    public void onCompleted() {
      logger.info("sendRawFile completed");
      // called by client after the entire file is sent
      try {
        outputStream.close();
        // TOOD: should we send fileSize copied?
        long endTime = System.nanoTime();
        long totalTimeInMilliSeoncds = (endTime - startTime) / (1000 * 1000);
        responseObserver.onNext(
            TransferStatus.newBuilder()
                .setCode(TransferStatusCode.Done)
                .setMessage(String.valueOf(totalTimeInMilliSeoncds))
                .build());
        responseObserver.onCompleted();
      } catch (IOException e) {
        logger.warn("error while trying to close outputStream", e);
        responseObserver.onError(e);
      }
    }
  }
}
