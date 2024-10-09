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

import com.yelp.nrtsearch.server.grpc.ForceMergeRequest;
import com.yelp.nrtsearch.server.grpc.ForceMergeResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceMergeHandler extends Handler<ForceMergeRequest, ForceMergeResponse> {
  private static final Logger logger = LoggerFactory.getLogger(ForceMergeHandler.class);
  private static ForceMergeHandler instance;

  public ForceMergeHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      ForceMergeRequest forceMergeRequest, StreamObserver<ForceMergeResponse> responseObserver) {
    logger.info("Received force merge request: {}", forceMergeRequest);
    if (forceMergeRequest.getIndexName().isEmpty()) {
      responseObserver.onError(new IllegalArgumentException("Index name in request is empty"));
      return;
    }
    if (forceMergeRequest.getMaxNumSegments() == 0) {
      responseObserver.onError(new IllegalArgumentException("Cannot have 0 max segments"));
      return;
    }

    try {
      IndexState indexState = getGlobalState().getIndexOrThrow(forceMergeRequest.getIndexName());
      ShardState shardState = indexState.getShards().get(0);
      logger.info("Beginning force merge for index: {}", forceMergeRequest.getIndexName());
      shardState.writer.forceMerge(
          forceMergeRequest.getMaxNumSegments(), forceMergeRequest.getDoWait());
    } catch (IOException e) {
      logger.warn("Error during force merge for index {} ", forceMergeRequest.getIndexName(), e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "Error during force merge for index " + forceMergeRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
      return;
    }

    ForceMergeResponse.Status status =
        forceMergeRequest.getDoWait()
            ? ForceMergeResponse.Status.FORCE_MERGE_COMPLETED
            : ForceMergeResponse.Status.FORCE_MERGE_SUBMITTED;
    logger.info("Force merge status: {}", status);
    ForceMergeResponse response = ForceMergeResponse.newBuilder().setStatus(status).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
