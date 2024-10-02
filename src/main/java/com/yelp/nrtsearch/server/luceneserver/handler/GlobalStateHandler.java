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

import com.yelp.nrtsearch.server.grpc.GlobalStateRequest;
import com.yelp.nrtsearch.server.grpc.GlobalStateResponse;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalStateHandler extends Handler<GlobalStateRequest, GlobalStateResponse> {
  private static final Logger logger = LoggerFactory.getLogger(GlobalStateHandler.class);

  public GlobalStateHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      GlobalStateRequest request, StreamObserver<GlobalStateResponse> responseObserver) {
    try {
      responseObserver.onNext(
          GlobalStateResponse.newBuilder().setGlobalState(getGlobalState().getStateInfo()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error while trying to get global state", e);
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription("error while trying to get global state")
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
