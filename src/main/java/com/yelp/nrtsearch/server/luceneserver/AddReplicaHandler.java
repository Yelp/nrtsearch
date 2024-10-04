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
import com.yelp.nrtsearch.server.luceneserver.handler.Handler;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
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
  public void handle(
      AddReplicaRequest addReplicaRequest, StreamObserver<AddReplicaResponse> responseObserver) {
    try {
      IndexStateManager indexStateManager =
          getGlobalState().getIndexStateManager(addReplicaRequest.getIndexName());
      checkIndexId(addReplicaRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

      IndexState indexState = indexStateManager.getCurrent();
      AddReplicaResponse reply = handle(indexState, addReplicaRequest);
      logger.info("AddReplicaHandler returned " + reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      logger.warn("error while trying addReplicas " + addReplicaRequest.getIndexName(), e);
      responseObserver.onError(e);
    } catch (Exception e) {
      logger.warn("error while trying addReplicas " + addReplicaRequest.getIndexName(), e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to addReplicas for index: "
                      + addReplicaRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
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
