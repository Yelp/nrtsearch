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

import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexHandler extends Handler<CreateIndexRequest, CreateIndexResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CreateIndexHandler.class);
  private static CreateIndexHandler instance;

  public CreateIndexHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState) {
    instance = new CreateIndexHandler(globalState);
  }

  public static CreateIndexHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(CreateIndexRequest req, StreamObserver<CreateIndexResponse> responseObserver) {
    logger.info("Received create index request: {}", req);
    String indexName = req.getIndexName();
    String validIndexNameRegex = "[A-z0-9_-]+";
    if (!indexName.matches(validIndexNameRegex)) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Index name %s is invalid - must contain only a-z, A-Z or 0-9", indexName))
              .asRuntimeException());
      return;
    }

    try {
      getGlobalState().createIndex(req);
      String response = String.format("Created Index name: %s", indexName);
      CreateIndexResponse reply = CreateIndexResponse.newBuilder().setResponse(response).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      logger.warn("invalid IndexName: " + indexName, e);
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription("invalid indexName: " + indexName)
              .augmentDescription("IllegalArgumentException()")
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      logger.warn("error while trying to save index state to disk for indexName: " + indexName, e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to save index state to disk for indexName: " + indexName)
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    }
  }
}
