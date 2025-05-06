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
package com.yelp.nrtsearch.server.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.CommitResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.ProtoMessagePrinter;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitHandler extends Handler<CommitRequest, CommitResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);

  public CommitHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(CommitRequest commitRequest, StreamObserver<CommitResponse> responseObserver) {
    try {
      getGlobalState()
          .submitCommitTask(
              Context.current()
                  .wrap(
                      () -> {
                        try {
                          IndexState indexState = getIndexState(commitRequest.getIndexName());
                          long gen = indexState.commit();
                          CommitResponse reply =
                              CommitResponse.newBuilder()
                                  .setGen(gen)
                                  .setPrimaryId(getGlobalState().getEphemeralId())
                                  .build();
                          logger.debug(
                              "CommitHandler committed to index: {} for sequenceId: {}",
                              commitRequest.getIndexName(),
                              gen);
                          responseObserver.onNext(reply);
                          responseObserver.onCompleted();
                        } catch (Exception e) {
                          String requestStr;
                          try {
                            requestStr =
                                ProtoMessagePrinter.omittingInsignificantWhitespace()
                                    .print(commitRequest);
                          } catch (InvalidProtocolBufferException ignored) {
                            // Ignore as invalid proto would have thrown an exception earlier
                            requestStr = commitRequest.toString();
                          }
                          logger.warn(
                              String.format("Error handling commit request: %s", requestStr), e);
                          if (e instanceof StatusRuntimeException) {
                            responseObserver.onError(e);
                          } else {
                            responseObserver.onError(
                                Status.INTERNAL
                                    .withDescription(
                                        "Error while trying to commit to index: "
                                            + commitRequest.getIndexName())
                                    .augmentDescription(e.getMessage())
                                    .asRuntimeException());
                          }
                        }
                      }));
    } catch (RejectedExecutionException e) {
      logger.error(
          "Threadpool is full, unable to submit commit to index {}", commitRequest.getIndexName());
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED
              .withDescription(
                  "Threadpool is full, unable to submit commit to index: "
                      + commitRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
