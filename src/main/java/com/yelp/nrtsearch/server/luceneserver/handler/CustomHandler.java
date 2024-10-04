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

import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.luceneserver.custom.request.CustomRequestProcessor;
import com.yelp.nrtsearch.server.luceneserver.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHandler extends Handler<CustomRequest, CustomResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CustomHandler.class);

  public CustomHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(CustomRequest request, StreamObserver<CustomResponse> responseObserver) {
    logger.info("Received custom request: {}", request);
    try {
      CustomResponse response = CustomRequestProcessor.processCustomRequest(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String error =
          String.format("Error processing custom request %s, error: %s", request, e.getMessage());
      logger.error(error);
      responseObserver.onError(Status.INTERNAL.withDescription(error).withCause(e).asException());
    }
  }
}
