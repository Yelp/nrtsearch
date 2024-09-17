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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GlobalStateTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testEmptyGlobalState() throws IOException {
    TestServer server = TestServer.builder(folder).build();
    GlobalStateResponse response =
        server.getClient().getBlockingStub().globalState(GlobalStateRequest.newBuilder().build());
    GlobalStateInfo expected = GlobalStateInfo.newBuilder().build();
    assertEquals(expected, response.getGlobalState());
  }

  @Test
  public void testGlobalStateWithIndex() throws IOException {
    TestServer server = TestServer.builder(folder).build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    String indexId = server.getGlobalState().getIndexStateManager("test_index").getIndexId();
    GlobalStateResponse response =
        server.getClient().getBlockingStub().globalState(GlobalStateRequest.newBuilder().build());
    GlobalStateInfo expected =
        GlobalStateInfo.newBuilder()
            .setGen(2)
            .putIndices(
                "test_index", IndexGlobalState.newBuilder().setId(indexId).setStarted(true).build())
            .build();
    assertEquals(expected, response.getGlobalState());
  }
}
