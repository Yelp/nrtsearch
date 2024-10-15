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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.NodeInfoRequest;
import com.yelp.nrtsearch.server.grpc.NodeInfoResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class NodeInfoHandlerTest {

  @Test
  public void testNodeInfoHandler() {
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getNodeName()).thenReturn("nodeName");
    when(mockGlobalState.getServiceName()).thenReturn("serviceName");
    when(mockGlobalState.getHostName()).thenReturn("hostName");
    when(mockGlobalState.getEphemeralId()).thenReturn("ephemeralId");
    NodeInfoHandler nodeInfoHandler = new NodeInfoHandler(mockGlobalState);
    StreamObserver<NodeInfoResponse> mockResponseObserver = mock(StreamObserver.class);

    nodeInfoHandler.handle(NodeInfoRequest.newBuilder().build(), mockResponseObserver);

    ArgumentCaptor<NodeInfoResponse> captor = ArgumentCaptor.forClass(NodeInfoResponse.class);
    verify(mockResponseObserver, times(1)).onNext(captor.capture());
    NodeInfoResponse response = captor.getValue();
    assertEquals("nodeName", response.getNodeName());
    assertEquals("serviceName", response.getServiceName());
    assertEquals("hostName", response.getHostName());
    assertEquals("ephemeralId", response.getEphemeralId());
    assertFalse(response.getVersion().isEmpty());
    verify(mockResponseObserver, times(1)).onCompleted();
  }
}
