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

import com.yelp.nrtsearch.server.grpc.NewNRTPoint;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewNRTPointHandler extends Handler<NewNRTPoint, TransferStatus> {
  private static final Logger logger = LoggerFactory.getLogger(NewNRTPointHandler.class);
  private final boolean verifyIndexId;

  public NewNRTPointHandler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public void handle(NewNRTPoint request, StreamObserver<TransferStatus> responseObserver) {
    try {
      IndexStateManager indexStateManager =
          getGlobalState().getIndexStateManager(request.getIndexName());
      checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

      IndexState indexState = indexStateManager.getCurrent();
      TransferStatus reply = handle(indexState, request);
      logger.debug(
          "NewNRTPointHandler returned status "
              + reply.getCode()
              + " message: "
              + reply.getMessage());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      logger.warn(
          String.format(
              "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
              request.getIndexName(), request.getVersion(), request.getPrimaryGen()),
          e);
      responseObserver.onError(e);
    } catch (Exception e) {
      logger.warn(
          String.format(
              "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
              request.getIndexName(), request.getVersion(), request.getPrimaryGen()),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format(
                      "error on newNRTPoint for indexName: %s, for version: %s, primaryGen: %s",
                      request.getIndexName(), request.getVersion(), request.getPrimaryGen()))
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private TransferStatus handle(IndexState indexState, NewNRTPoint newNRTPointRequest) {
    ShardState shardState = indexState.getShard(0);
    if (shardState.isReplica() == false) {
      throw new IllegalArgumentException(
          "index \""
              + newNRTPointRequest.getIndexName()
              + "\" is not a replica or was not started yet");
    }

    long version = newNRTPointRequest.getVersion();
    long newPrimaryGen = newNRTPointRequest.getPrimaryGen();
    try {
      shardState.nrtReplicaNode.newNRTPoint(newPrimaryGen, version);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return TransferStatus.newBuilder()
        .setMessage(
            "Replica kicked off a job (runs in the background) to copy files across, and open a new reader once that's done.")
        .setCode(TransferStatusCode.Done)
        .build();
  }
}
