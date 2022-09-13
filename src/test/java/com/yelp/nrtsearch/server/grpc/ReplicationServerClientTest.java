/*
 * Copyright 2022 Yelp Inc.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.grpc.LuceneServer.ReplicationServerImpl;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient.DiscoveryFileAndPort;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationServerClientTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TEST_FILE = "test_nodes.json";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private URI testFileURI() {
    try {
      return new URI(Paths.get(folder.getRoot().toString(), TEST_FILE).toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeNodeFile(List<Node> nodes) throws IOException {
    String filePathStr = Paths.get(folder.getRoot().toString(), TEST_FILE).toString();
    String fileStr = OBJECT_MAPPER.writeValueAsString(nodes);
    try (FileOutputStream outputStream = new FileOutputStream(filePathStr)) {
      outputStream.write(fileStr.getBytes());
    }
  }

  private Server getBasicReplicationServer() throws IOException {
    // we only need to test connectivity for now
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getIndex(any(String.class))).thenThrow(new RuntimeException("Expected"));

    return ServerBuilder.forPort(0)
        .addService(new ReplicationServerImpl(mockGlobalState))
        .build()
        .start();
  }

  private void verifyConnected(ReplicationServerClient client) {
    try {
      client.getConnectedNodes("test_index");
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals("INTERNAL: error on GetNodesInfoHandler\nExpected", e.getMessage());
    }
  }

  @Test
  public void testConnectWithDiscoveryFile() throws IOException {
    Server replicationServer = getBasicReplicationServer();
    ReplicationServerClient client = null;
    try {
      writeNodeFile(Collections.singletonList(new Node("localhost", replicationServer.getPort())));
      client = new ReplicationServerClient(new DiscoveryFileAndPort(testFileURI().getPath(), 0));
      verifyConnected(client);
    } finally {
      if (client != null) {
        client.close();
      }
      replicationServer.shutdown();
    }
  }

  @Test
  public void testConnectWithDiscoveryFileIgnoreUnknown() throws IOException {
    Server replicationServer = getBasicReplicationServer();
    ReplicationServerClient client = null;
    try {
      String filePathStr = Paths.get(folder.getRoot().toString(), TEST_FILE).toString();
      String fileStr =
          "[{\"host\":\"localhost\",\"port\":"
              + replicationServer.getPort()
              + ",\"other\":\"property\"}]";
      try (FileOutputStream outputStream = new FileOutputStream(filePathStr)) {
        outputStream.write(fileStr.getBytes());
      }
      client = new ReplicationServerClient(new DiscoveryFileAndPort(testFileURI().getPath(), 0));
      verifyConnected(client);
    } finally {
      if (client != null) {
        client.close();
      }
      replicationServer.shutdown();
    }
  }

  @Test
  public void testDiscoveryFilePrimaryChange() throws IOException {
    Server replicationServer = getBasicReplicationServer();
    ReplicationServerClient client = null;
    try {
      try {
        writeNodeFile(
            Collections.singletonList(new Node("localhost", replicationServer.getPort())));
        client =
            new ReplicationServerClient(new DiscoveryFileAndPort(testFileURI().getPath(), 0), 100);
        verifyConnected(client);
      } finally {
        replicationServer.shutdown();
      }
      replicationServer = getBasicReplicationServer();
      try {
        writeNodeFile(
            Collections.singletonList(new Node("localhost", replicationServer.getPort())));
        boolean success = false;
        // try for 10s
        for (int i = 0; i < 100; ++i) {
          try {
            verifyConnected(client);
            success = true;
            break;
          } catch (Throwable ignore) {
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException ignore) {
          }
        }
        assertTrue(success);
      } finally {
        replicationServer.shutdown();
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
