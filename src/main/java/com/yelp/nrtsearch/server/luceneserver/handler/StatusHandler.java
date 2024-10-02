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

import com.yelp.nrtsearch.server.grpc.HealthCheckRequest;
import com.yelp.nrtsearch.server.grpc.HealthCheckResponse;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusHandler extends Handler<HealthCheckRequest, HealthCheckResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StatusHandler.class);
  private static StatusHandler instance;

  public StatusHandler(GlobalState globalState) {
    super(globalState);
  }

  public static void initialize(GlobalState globalState) {
    instance = new StatusHandler(globalState);
  }

  public static StatusHandler getInstance() {
    return instance;
  }

  @Override
  public void handle(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    try {
      HealthCheckResponse reply =
          HealthCheckResponse.newBuilder().setHealth(TransferStatusCode.Done).build();
      logger.debug("HealthCheckResponse returned " + reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error while trying to get status", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("error while trying to get status")
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
