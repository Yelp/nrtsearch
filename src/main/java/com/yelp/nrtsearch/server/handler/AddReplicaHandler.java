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

import com.yelp.nrtsearch.server.grpc.AddReplicaRequest;
import com.yelp.nrtsearch.server.grpc.AddReplicaResponse;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddReplicaHandler extends Handler<AddReplicaRequest, AddReplicaResponse> {
  private static final Logger logger = LoggerFactory.getLogger(AddReplicaHandler.class);

  private final boolean useKeepAlive;
  private final boolean verifyIndexId;

  public AddReplicaHandler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.useKeepAlive = globalState.getConfiguration().getUseKeepAliveForReplication();
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public AddReplicaResponse handle(AddReplicaRequest addReplicaRequest) throws Exception {
    IndexStateManager indexStateManager = getIndexStateManager(addReplicaRequest.getIndexName());
    checkIndexId(addReplicaRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

    IndexState indexState = indexStateManager.getCurrent();
    AddReplicaResponse reply = handle(indexState, addReplicaRequest);
    logger.info("AddReplicaHandler returned {}", reply);
    return reply;
  }

  private AddReplicaResponse handle(IndexState indexState, AddReplicaRequest addReplicaRequest) {
    ShardState shardState = indexState.getShard(0);
    if (!shardState.isPrimary()) {
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
              addReplicaRequest.getHostName(), addReplicaRequest.getPort(), useKeepAlive));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return AddReplicaResponse.newBuilder().setOk("ok").build();
  }
}
