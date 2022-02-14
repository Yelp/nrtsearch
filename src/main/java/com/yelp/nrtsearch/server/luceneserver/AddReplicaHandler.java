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

import com.yelp.nrtsearch.server.grpc.AddReplicaRequest;
import com.yelp.nrtsearch.server.grpc.AddReplicaResponse;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import java.io.IOException;

public class AddReplicaHandler implements Handler<AddReplicaRequest, AddReplicaResponse> {
  @Override
  public AddReplicaResponse handle(IndexState indexState, AddReplicaRequest addReplicaRequest) {
    ShardState shardState = indexState.getShard(0);
    if (shardState.isPrimary() == false) {
      throw new IllegalArgumentException(
          "index \"" + indexState.getName() + "\" was not started or is not a primary");
    }

    if (!isValidMagicHeader(addReplicaRequest.getMagicNumber())) {
      throw new RuntimeException("AddReplica invoked with Invalid Magic Number");
    }
    try {
      shardState.nrtPrimaryNode.addReplica(
          addReplicaRequest.getReplicaId(),
          // channel for primary to talk to replica
          new ReplicationServerClient(
              addReplicaRequest.getHostName(), addReplicaRequest.getPort()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return AddReplicaResponse.newBuilder().setOk("ok").build();
  }
}
