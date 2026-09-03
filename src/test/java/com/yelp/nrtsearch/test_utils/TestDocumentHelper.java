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
package com.yelp.nrtsearch.test_utils;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class TestDocumentHelper {

  public static AddDocumentResponse addDocuments(
      LuceneServerGrpc.LuceneServerStub stub, Stream<AddDocumentRequest> requestStream) {
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicReference<AddDocumentResponse> response = new AtomicReference<>();
    AtomicReference<RuntimeException> exception = new AtomicReference<>();
    StreamObserver<AddDocumentResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AddDocumentResponse value) {
            response.set(value);
          }

          @Override
          public void onError(Throwable t) {
            exception.set(new RuntimeException(t));
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            finishLatch.countDown();
          }
        };
    StreamObserver<AddDocumentRequest> requestObserver = stub.addDocuments(responseObserver);
    try {
      requestStream.forEach(requestObserver::onNext);
    } catch (RuntimeException e) {
      requestObserver.onError(e);
      throw e;
    }
    requestObserver.onCompleted();
    try {
      if (!finishLatch.await(20, TimeUnit.SECONDS)) {
        throw new RuntimeException("addDocuments can not finish within 20 seconds");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (exception.get() != null) {
      throw exception.get();
    }
    return response.get();
  }
}
