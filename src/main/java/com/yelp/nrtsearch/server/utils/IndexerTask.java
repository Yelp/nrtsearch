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
package com.yelp.nrtsearch.server.utils;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class IndexerTask {
  private static final Logger logger = Logger.getLogger(IndexerTask.class.getName());
  private String genId;

  public Long index(
      LuceneServerClient luceneServerClient, Stream<AddDocumentRequest> addDocumentRequestStream)
      throws Exception {
    String threadId = Thread.currentThread().getName() + Thread.currentThread().getId();

    final CountDownLatch finishLatch = new CountDownLatch(1);

    StreamObserver<AddDocumentResponse> responseObserver =
        new StreamObserver<>() {

          @Override
          public void onNext(AddDocumentResponse value) {
            // Note that Server sends back only 1 message (Unary mode i.e. Server calls its onNext
            // only once
            // which is when it is done with indexing the entire stream), which means this method
            // should be
            // called only once.
            logger.fine(
                String.format(
                    "Received response for genId: %s on threadId: %s", value.getGenId(), threadId));
            genId = value.getGenId();
          }

          @Override
          public void onError(Throwable t) {
            logger.log(Level.SEVERE, t.getMessage(), t);
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            logger.fine(
                String.format("Received final response from server on threadId: %s", threadId));
            finishLatch.countDown();
          }
        };

    // The responseObserver handles responses from the server (i.e. 1 onNext and 1 completed)
    // The requestObserver handles the sending of stream of client requests to server (i.e. multiple
    // onNext and 1 completed)
    StreamObserver<AddDocumentRequest> requestObserver =
        luceneServerClient.getAsyncStub().addDocuments(responseObserver);
    try {
      addDocumentRequestStream.forEach(
          addDocumentRequest -> requestObserver.onNext(addDocumentRequest));
    } catch (RuntimeException e) {
      // Cancel RPC
      requestObserver.onError(e);
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();

    logger.fine(
        String.format("sent async addDocumentsRequest to server on threadId: %s", threadId));

    // Receiving happens asynchronously, so block here for 5 minutes
    if (!finishLatch.await(5, TimeUnit.MINUTES)) {
      logger.log(
          Level.WARNING,
          String.format("addDocuments can not finish within 5 minutes on threadId: %s", threadId));
    }
    return Long.valueOf(genId);
  }
}
