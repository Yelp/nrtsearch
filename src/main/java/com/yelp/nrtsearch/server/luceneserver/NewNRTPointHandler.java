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

import com.yelp.nrtsearch.server.grpc.NewNRTPoint;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import java.io.IOException;

public class NewNRTPointHandler implements Handler<NewNRTPoint, TransferStatus> {

  @Override
  public TransferStatus handle(IndexState indexState, NewNRTPoint newNRTPointRequest)
      throws HandlerException {
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
