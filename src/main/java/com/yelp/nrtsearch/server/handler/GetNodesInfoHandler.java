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

import com.yelp.nrtsearch.server.grpc.GetNodesRequest;
import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.nrt.NRTPrimaryNode;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNodesInfoHandler extends Handler<GetNodesRequest, GetNodesResponse> {
  private static final Logger logger = LoggerFactory.getLogger(GetNodesInfoHandler.class);

  public GetNodesInfoHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public GetNodesResponse handle(GetNodesRequest getNodesRequest) throws Exception {
    IndexState indexState = getIndexState(getNodesRequest.getIndexName());
    GetNodesResponse reply = handle(indexState);
    logger.debug("GetNodesInfoHandler returned GetNodeResponse of size {}", reply.getNodesCount());
    return reply;
  }

  private GetNodesResponse handle(IndexState indexState) {
    GetNodesResponse.Builder builder = GetNodesResponse.newBuilder();
    ShardState shardState = indexState.getShard(0);
    if (!shardState.isPrimary() || !shardState.isStarted()) {
      logger.warn("index \"{}\" is not a primary or was not started yet", indexState.getName());
    } else { // shard is a primary and started
      Collection<NRTPrimaryNode.ReplicaDetails> replicasInfo =
          shardState.nrtPrimaryNode.getNodesInfo();
      for (NRTPrimaryNode.ReplicaDetails replica : replicasInfo) {
        HostPort hostPort = replica.getHostPort();
        builder.addNodes(
            NodeInfo.newBuilder()
                .setHostname(hostPort.getHostName())
                .setPort(hostPort.getPort())
                .setNodeName(replica.getNodeName())
                .build());
      }
    }
    return builder.build();
  }
}
