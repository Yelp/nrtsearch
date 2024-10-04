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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.GetNodesRequest;
import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.luceneserver.handler.Handler;
import com.yelp.nrtsearch.server.luceneserver.index.IndexState;
import com.yelp.nrtsearch.server.luceneserver.index.ShardState;
import com.yelp.nrtsearch.server.luceneserver.nrt.NRTPrimaryNode;
import com.yelp.nrtsearch.server.luceneserver.state.GlobalState;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNodesInfoHandler extends Handler<GetNodesRequest, GetNodesResponse> {
  private static final Logger logger = LoggerFactory.getLogger(GetNodesInfoHandler.class);

  public GetNodesInfoHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      GetNodesRequest getNodesRequest, StreamObserver<GetNodesResponse> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndex(getNodesRequest.getIndexName());
      GetNodesResponse reply = handle(indexState);
      logger.debug("GetNodesInfoHandler returned GetNodeResponse of size " + reply.getNodesCount());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("error on GetNodesInfoHandler", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("error on GetNodesInfoHandler")
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private GetNodesResponse handle(IndexState indexState) {
    GetNodesResponse.Builder builder = GetNodesResponse.newBuilder();
    ShardState shardState = indexState.getShard(0);
    if (!shardState.isPrimary() || !shardState.isStarted()) {
      logger.warn("index \"" + indexState.getName() + "\" is not a primary or was not started yet");
    } else { // shard is a primary and started
      Collection<NRTPrimaryNode.ReplicaDetails> replicasInfo =
          shardState.nrtPrimaryNode.getNodesInfo();
      for (NRTPrimaryNode.ReplicaDetails replica : replicasInfo) {
        HostPort hostPort = replica.getHostPort();
        builder.addNodes(
            NodeInfo.newBuilder()
                .setHostname(hostPort.getHostName())
                .setPort(hostPort.getPort())
                .build());
      }
    }
    return builder.build();
  }
}
