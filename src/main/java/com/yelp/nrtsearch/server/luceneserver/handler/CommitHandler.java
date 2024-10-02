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

import com.yelp.nrtsearch.server.grpc.CommitRequest;
import com.yelp.nrtsearch.server.grpc.CommitResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitHandler extends Handler<CommitRequest, CommitResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CommitHandler.class);
  private static CommitHandler instance;

  public CommitHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState) {
    instance = new CommitHandler(globalState);
  }

  public static CommitHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(CommitRequest commitRequest, StreamObserver<CommitResponse> responseObserver) {
    try {
      getGlobalState()
          .submitIndexingTask(
              Context.current()
                  .wrap(
                      () -> {
                        try {
                          IndexState indexState =
                              getGlobalState().getIndex(commitRequest.getIndexName());
                          long gen = indexState.commit();
                          CommitResponse reply =
                              CommitResponse.newBuilder()
                                  .setGen(gen)
                                  .setPrimaryId(getGlobalState().getEphemeralId())
                                  .build();
                          logger.debug(
                              String.format(
                                  "CommitHandler committed to index: %s for sequenceId: %s",
                                  commitRequest.getIndexName(), gen));
                          responseObserver.onNext(reply);
                          responseObserver.onCompleted();
                        } catch (IOException e) {
                          logger.warn(
                              "error while trying to read index state dir for indexName: "
                                  + commitRequest.getIndexName(),
                              e);
                          responseObserver.onError(
                              Status.INTERNAL
                                  .withDescription(
                                      "error while trying to read index state dir for indexName: "
                                          + commitRequest.getIndexName())
                                  .augmentDescription(e.getMessage())
                                  .withCause(e)
                                  .asRuntimeException());
                        } catch (Exception e) {
                          logger.warn(
                              "error while trying to commit to  index "
                                  + commitRequest.getIndexName(),
                              e);
                          if (e instanceof StatusRuntimeException) {
                            responseObserver.onError(e);
                          } else {
                            responseObserver.onError(
                                Status.UNKNOWN
                                    .withDescription(
                                        "error while trying to commit to index: "
                                            + commitRequest.getIndexName())
                                    .augmentDescription(e.getMessage())
                                    .asRuntimeException());
                          }
                        }
                        return null;
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
