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

import com.yelp.nrtsearch.server.Version;
import com.yelp.nrtsearch.server.grpc.NodeInfoRequest;
import com.yelp.nrtsearch.server.grpc.NodeInfoResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.stub.StreamObserver;

public class NodeInfoHandler extends Handler<NodeInfoRequest, NodeInfoResponse> {
  public NodeInfoHandler(GlobalState globalState) {
    super(globalState);
  }

  public void handle(NodeInfoRequest request, StreamObserver<NodeInfoResponse> responseObserver) {
    GlobalState globalState = getGlobalState();
    NodeInfoResponse.Builder builder = NodeInfoResponse.newBuilder();
    builder.setNodeName(globalState.getNodeName());
    builder.setServiceName(globalState.getServiceName());
    builder.setHostName(globalState.getHostName());
    builder.setVersion(Version.CURRENT.toString());
    builder.setEphemeralId(globalState.getEphemeralId());

    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }
}
