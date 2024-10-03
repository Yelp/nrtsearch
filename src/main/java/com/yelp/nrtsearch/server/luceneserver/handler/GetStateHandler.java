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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.google.gson.JsonObject;
import com.yelp.nrtsearch.server.grpc.StateRequest;
import com.yelp.nrtsearch.server.grpc.StateResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to StateHandler
public class GetStateHandler extends Handler<StateRequest, StateResponse> {
  private static final Logger logger = LoggerFactory.getLogger(GetStateHandler.class);

  public GetStateHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(StateRequest request, StreamObserver<StateResponse> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndex(request.getIndexName());
      StateResponse reply = handle(indexState);
      logger.debug("GetStateHandler returned " + reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error while trying to get state for index " + request.getIndexName(), e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "error while trying to get state for index " + request.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private StateResponse handle(IndexState indexState) throws HandlerException {
    StateResponse.Builder builder = StateResponse.newBuilder();
    JsonObject savedState = new JsonObject();
    try {
      savedState.add("state", indexState.getSaveState());
    } catch (IOException e) {
      logger.error("Could not load state for index " + indexState.getName(), e);
      throw new GetStateHandlerException(e);
    }
    builder.setResponse(savedState.toString());
    return builder.build();
  }

  public static class GetStateHandlerException extends Handler.HandlerException {

    public GetStateHandlerException(Throwable err) {
      super(err);
    }
  }
}
