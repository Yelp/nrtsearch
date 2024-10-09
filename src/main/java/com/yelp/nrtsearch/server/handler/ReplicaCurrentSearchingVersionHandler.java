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
package com.yelp.nrtsearch.server.handler;

import com.yelp.nrtsearch.server.grpc.IndexName;
import com.yelp.nrtsearch.server.grpc.SearcherVersion;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaCurrentSearchingVersionHandler extends Handler<IndexName, SearcherVersion> {
  private static final Logger logger =
      LoggerFactory.getLogger(ReplicaCurrentSearchingVersionHandler.class);

  public ReplicaCurrentSearchingVersionHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(IndexName indexNameRequest, StreamObserver<SearcherVersion> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndexOrThrow(indexNameRequest.getIndexName());
      SearcherVersion reply = handle(indexState, indexNameRequest);
      logger.info("ReplicaCurrentSearchingVersionHandler returned version " + reply.getVersion());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn(
          String.format(
              "error on getCurrentSearcherVersion for indexName: %s",
              indexNameRequest.getIndexName()),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format(
                      "error on getCurrentSearcherVersion for indexName: %s",
                      indexNameRequest.getIndexName()))
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private SearcherVersion handle(IndexState indexState, IndexName indexNameRequest) {
    ShardState shardState = indexState.getShard(0);
    if (!shardState.isReplica() || !shardState.isStarted()) {
      throw new IllegalArgumentException(
          "index \""
              + indexNameRequest.getIndexName()
              + "\" is not a replica or was not started yet");
    }
    try {
      long currentSearchingVersion = shardState.nrtReplicaNode.getCurrentSearchingVersion();
      return SearcherVersion.newBuilder().setVersion(currentSearchingVersion).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
