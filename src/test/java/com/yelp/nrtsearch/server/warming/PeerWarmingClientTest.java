/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class PeerWarmingClientTest {
  private static final String INDEX = "test_index";
  private static final HostPort SELF = new HostPort("self", 1000);
  private static final byte[] QUERIES =
      "{\"indexName\":\"test_index\"}\n".getBytes(StandardCharsets.UTF_8);

  private ReplicationServerClient primary;
  private final Map<String, ReplicationServerClient> peers = new HashMap<>();
  private final List<String> contacted = new ArrayList<>();

  @Before
  public void setup() {
    primary = mock(ReplicationServerClient.class);
  }

  private static NodeInfo node(String host, int port) {
    return NodeInfo.newBuilder().setHostname(host).setPort(port).setNodeName("replica").build();
  }

  private void setNodes(NodeInfo... nodes) {
    when(primary.getConnectedNodes(INDEX))
        .thenReturn(GetNodesResponse.newBuilder().addAllNodes(List.of(nodes)).build());
  }

  private ReplicationServerClient peer(String host) {
    ReplicationServerClient client = mock(ReplicationServerClient.class);
    peers.put(host, client);
    return client;
  }

  private static void returns(ReplicationServerClient client, byte[] content) {
    when(client.getWarmingQueries(eq(INDEX), anyInt(), anyLong(), anyInt(), anyInt(), anyLong()))
        .thenReturn(content);
  }

  private static void fails(ReplicationServerClient client, Status status) {
    when(client.getWarmingQueries(eq(INDEX), anyInt(), anyLong(), anyInt(), anyInt(), anyLong()))
        .thenThrow(status.asRuntimeException());
  }

  private PeerWarmingClient client(ReplicationServerClient primaryClient, int maxCandidates) {
    WarmerConfig config =
        new WarmerConfig(
            500,
            1,
            0,
            true,
            false,
            false,
            0,
            0,
            0,
            0,
            0,
            WarmerConfig.WarmingStrategy.PEER,
            100,
            WarmerConfig.WarmingStrategy.S3,
            10,
            5,
            1000,
            2000,
            maxCandidates);
    return new PeerWarmingClient(
        INDEX,
        primaryClient,
        SELF,
        config,
        (host, port) -> {
          contacted.add(host + ":" + port);
          return peers.get(host);
        });
  }

  @Test
  public void testNoPrimary() {
    assertTrue(client(null, 3).fetch().isEmpty());
  }

  @Test
  public void testPrimaryUnavailable() {
    when(primary.getConnectedNodes(INDEX)).thenThrow(Status.UNAVAILABLE.asRuntimeException());
    assertTrue(client(primary, 3).fetch().isEmpty());
  }

  @Test
  public void testOnlySelf() {
    setNodes(node("self", 1000));
    assertTrue(client(primary, 3).fetch().isEmpty());
    assertEquals(List.of(), contacted);
  }

  @Test
  public void testFirstQualifyingPeerInConnectionOrder() {
    setNodes(node("a", 1), node("self", 1000), node("b", 2), node("c", 3));
    fails(peer("a"), Status.FAILED_PRECONDITION.withDescription("uptime"));
    returns(peer("b"), QUERIES);
    returns(peer("c"), QUERIES);

    Optional<PeerWarmingQuerySource.PeerWarmingQueries> result = client(primary, 3).fetch();

    assertTrue(result.isPresent());
    assertArrayEquals(QUERIES, result.get().content());
    assertEquals("replica (b:2)", result.get().peer());
    // c is not needed, self is skipped
    assertEquals(List.of("a:1", "b:2"), contacted);
    verify(peers.get("b")).getWarmingQueries(INDEX, 500, 1000, 10, 5, 2000);
    verify(peers.get("a")).close();
    verify(peers.get("b")).close();
  }

  @Test
  public void testSkipsEmptyAndFailingPeers() {
    setNodes(node("a", 1), node("b", 2), node("c", 3));
    returns(peer("a"), new byte[0]);
    fails(peer("b"), Status.UNAVAILABLE);
    returns(peer("c"), QUERIES);

    Optional<PeerWarmingQuerySource.PeerWarmingQueries> result = client(primary, 3).fetch();

    assertTrue(result.isPresent());
    assertEquals(List.of("a:1", "b:2", "c:3"), contacted);
  }

  @Test
  public void testMaxCandidates() {
    setNodes(node("a", 1), node("b", 2), node("c", 3));
    fails(peer("a"), Status.FAILED_PRECONDITION);
    fails(peer("b"), Status.FAILED_PRECONDITION);
    returns(peer("c"), QUERIES);

    assertTrue(client(primary, 2).fetch().isEmpty());
    assertEquals(List.of("a:1", "b:2"), contacted);
  }
}
