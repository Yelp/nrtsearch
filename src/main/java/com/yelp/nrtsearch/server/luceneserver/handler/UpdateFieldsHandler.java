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

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.handlers.FieldUpdateHandler;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateFieldsHandler extends Handler<FieldDefRequest, FieldDefResponse> {
  private static final Logger logger = LoggerFactory.getLogger(UpdateFieldsHandler.class);
  private static UpdateFieldsHandler instance;

  public UpdateFieldsHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState) {
    instance = new UpdateFieldsHandler(globalState);
  }

  public static UpdateFieldsHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(
      FieldDefRequest fieldDefRequest, StreamObserver<FieldDefResponse> responseObserver) {
    logger.info("Received update fields request: {}", fieldDefRequest);
    try {
      IndexStateManager indexStateManager =
          getGlobalState().getIndexStateManager(fieldDefRequest.getIndexName());
      FieldDefResponse reply = FieldUpdateHandler.handle(indexStateManager, fieldDefRequest);
      logger.info("UpdateFieldsHandler registered fields " + reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (IOException e) {
      logger.warn(
          "error while trying to read index state dir for indexName: "
              + fieldDefRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to read index state dir for indexName: "
                      + fieldDefRequest.getIndexName())
              .augmentDescription("IOException()")
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      logger.warn(
          "error while trying to UpdateFieldsHandler for index " + fieldDefRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "error while trying to UpdateFieldsHandler for index: "
                      + fieldDefRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
