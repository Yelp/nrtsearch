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

import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.RefreshResponse;
import com.yelp.nrtsearch.server.luceneserver.index.IndexState;
import com.yelp.nrtsearch.server.luceneserver.index.ShardState;
import com.yelp.nrtsearch.server.luceneserver.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshHandler extends Handler<RefreshRequest, RefreshResponse> {
  private static final Logger logger = LoggerFactory.getLogger(RefreshHandler.class);

  public RefreshHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      RefreshRequest refreshRequest, StreamObserver<RefreshResponse> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndex(refreshRequest.getIndexName());
      final ShardState shardState = indexState.getShard(0);
      long t0 = System.nanoTime();
      shardState.maybeRefreshBlocking();
      long t1 = System.nanoTime();
      double refreshTimeMs = (t1 - t0) / 1000000.0;
      RefreshResponse reply = RefreshResponse.newBuilder().setRefreshTimeMS(refreshTimeMs).build();
      logger.info(
          String.format(
              "RefreshHandler refreshed index: %s in %f",
              refreshRequest.getIndexName(), refreshTimeMs));
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (IOException e) {
      logger.warn(
          "error while trying to read index state dir for indexName: "
              + refreshRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to read index state dir for indexName: "
                      + refreshRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      logger.warn("error while trying to refresh index " + refreshRequest.getIndexName(), e);
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(
                  "error while trying to refresh index: " + refreshRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
