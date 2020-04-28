/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.GetNodeResponse;
import com.yelp.nrtsearch.server.grpc.GetNodesRequest;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class GetNodesInfoHandler implements Handler<GetNodesRequest, GetNodeResponse> {
    Logger logger = LoggerFactory.getLogger(GetNodesInfoHandler.class);

    @Override
    public GetNodeResponse handle(IndexState indexState, GetNodesRequest getNodesRequest) throws HandlerException {
        GetNodeResponse.Builder builder = GetNodeResponse.newBuilder();
        ShardState shardState = indexState.getShard(0);
        if (!shardState.isPrimary() || !shardState.isStarted()) {
            logger.warn("index \"" + indexState.name + "\" is not a primary or was not started yet");
        } else { //shard is a primary and started
            Collection<NRTPrimaryNode.ReplicaDetails> replicasInfo = shardState.nrtPrimaryNode.getNodesInfo();
            for (NRTPrimaryNode.ReplicaDetails replica : replicasInfo) {
                ReplicationServerClient replicationServerClient = replica.getReplicationServerClient();
                builder.addNodes(NodeInfo.newBuilder()
                        .setHostname(replicationServerClient.getHost())
                        .setPort(replicationServerClient.getPort())
                        .build());
            }
        }
        return builder.build();
    }

}
