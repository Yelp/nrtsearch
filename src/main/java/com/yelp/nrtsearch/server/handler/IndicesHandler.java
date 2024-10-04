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

import com.yelp.nrtsearch.server.grpc.IndexStatsResponse;
import com.yelp.nrtsearch.server.grpc.IndicesRequest;
import com.yelp.nrtsearch.server.grpc.IndicesResponse;
import com.yelp.nrtsearch.server.grpc.StatsResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndicesHandler extends Handler<IndicesRequest, IndicesResponse> {
  private static final Logger logger = LoggerFactory.getLogger(IndicesHandler.class);

  public IndicesHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      IndicesRequest indicesRequest, StreamObserver<IndicesResponse> responseObserver) {
    try {
      IndicesResponse reply = getIndicesResponse(getGlobalState());
      logger.debug("IndicesRequestHandler returned " + reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error while trying to get indices stats", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("error while trying to get indices stats")
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private static IndicesResponse getIndicesResponse(GlobalState globalState) throws IOException {
    Set<String> indexNames = globalState.getIndexNames();
    IndicesResponse.Builder builder = IndicesResponse.newBuilder();
    for (String indexName : indexNames) {
      IndexState indexState = globalState.getIndex(indexName);
      if (indexState.isStarted()) {
        StatsResponse statsResponse = StatsHandler.process(indexState);
        builder.addIndicesResponse(
            IndexStatsResponse.newBuilder()
                .setIndexName(indexName)
                .setStatsResponse(statsResponse)
                .build());
      } else {
        builder.addIndicesResponse(
            IndexStatsResponse.newBuilder()
                .setIndexName(indexName)
                .setStatsResponse(StatsResponse.newBuilder().setState("not_started").build()));
      }
    }
    return builder.build();
  }
}
