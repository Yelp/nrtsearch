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
package com.yelp.nrtsearch.server.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessageV3;
import com.yelp.nrtsearch.server.grpc.LuceneServerStubBuilder;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for handlers that process requests and produce responses or provide a handler for
 * streaming responses. For a gRPC method x, create a class xHandler that extends Handler. Override
 * the {@link #handle(StreamObserver)} method for streaming responses, or the {@link
 * #handle(GeneratedMessageV3, StreamObserver)} method for unary responses. Initialize the handler
 * in {@link com.yelp.nrtsearch.server.grpc.LuceneServer.LuceneServerImpl} and call the appropriate
 * method on the handler in the gRPC call.
 *
 * @param <T> Request type
 * @param <S> Response type
 */
public abstract class Handler<T extends GeneratedMessageV3, S extends GeneratedMessageV3> {
  private static final Logger logger = LoggerFactory.getLogger(Handler.class);

  private final GlobalState globalState;

  public Handler(GlobalState globalState) {
    this.globalState = globalState;
  }

  protected GlobalState getGlobalState() {
    return globalState;
  }

  public void handle(T protoRequest, StreamObserver<S> responseObserver) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  public StreamObserver<T> handle(StreamObserver<S> responseObserver) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  /**
   * Set response compression on the provided {@link StreamObserver}. Should be a valid compression
   * type from the {@link LuceneServerStubBuilder#COMPRESSOR_REGISTRY}, or empty string for default.
   * Falls back to uncompressed on any error.
   *
   * @param compressionType compression type, or empty string
   * @param responseObserver observer to set compression on
   */
  protected void setResponseCompression(
      String compressionType, StreamObserver<?> responseObserver) {
    if (!compressionType.isEmpty()) {
      try {
        ServerCallStreamObserver<?> serverCallStreamObserver =
            (ServerCallStreamObserver<?>) responseObserver;
        serverCallStreamObserver.setCompression(compressionType);
      } catch (Exception e) {
        logger.warn("Unable to set response compression to type '" + compressionType + "' : " + e);
      }
    }
  }

  protected boolean isValidMagicHeader(int magicHeader) {
    return magicHeader == ReplicationServerClient.BINARY_MAGIC;
  }

  @VisibleForTesting
  public static void checkIndexId(String actual, String expected, boolean throwException) {
    if (!actual.equals(expected)) {
      String message =
          String.format("Index id mismatch, expected: %s, actual: %s", expected, actual);
      if (throwException) {
        throw Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException();
      } else {
        logger.warn(message);
      }
    }
  }

  public static class HandlerException extends Exception {
    public HandlerException(Throwable err) {
      super(err);
    }

    public HandlerException(String errorMessage) {
      super(errorMessage);
    }

    public HandlerException(String errorMessage, Throwable err) {
      super(errorMessage, err);
    }
  }
}
