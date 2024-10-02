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

import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartIndexHandler extends Handler<StartIndexRequest, StartIndexResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);
  private static StartIndexHandler instance;

  public StartIndexHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState) {
    instance = new StartIndexHandler(globalState);
  }

  public static StartIndexHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(
      StartIndexRequest startIndexRequest, StreamObserver<StartIndexResponse> responseObserver) {
    logger.info("Received start index request: {}", startIndexRequest);
    if (startIndexRequest.getIndexName().isEmpty()) {
      logger.warn("error while trying to start index with empty index name.");
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  String.format("error while trying to start index since indexName was empty."))
              .asRuntimeException());
      return;
    }
    try {
      StartIndexResponse reply = getGlobalState().startIndex(startIndexRequest);

      logger.info("StartIndexHandler returned " + reply.toString());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();

    } catch (IOException e) {
      logger.warn(
          "error while trying to read index state dir for indexName: "
              + startIndexRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to read index state dir for indexName: "
                      + startIndexRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      logger.warn("error while trying to start index " + startIndexRequest.getIndexName(), e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "error while trying to start index: " + startIndexRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
