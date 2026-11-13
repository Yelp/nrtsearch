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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Control calls from a replica to a primary that accepts calls but never answers (e.g. paused in a
 * long GC) must time out, so the replica can move on to a new primary.
 */
public class ReplicationClientHungPrimaryTest {
  private static final long TIMEOUT_MS = 300;
  private Server server;
  private ManagedChannel channel;
  private ReplicationServerClient client;

  /** Accepts every call and never completes it. */
  private static class HungPrimary extends ReplicationServerGrpc.ReplicationServerImplBase {
    @Override
    public void addReplicas(
        AddReplicaRequest request, StreamObserver<AddReplicaResponse> responseObserver) {}

    @Override
    public void recvCopyState(
        CopyStateRequest request, StreamObserver<CopyState> responseObserver) {}

    @Override
    public void getConnectedNodes(
        GetNodesRequest request, StreamObserver<GetNodesResponse> responseObserver) {}
  }

  @Before
  public void setUp() throws Exception {
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(new HungPrimary())
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).build();
    client = new ReplicationServerClient(channel, "hung", 0, "");
    client.setControlCallTimeoutMs(TIMEOUT_MS);
  }

  @After
  public void tearDown() throws Exception {
    channel.shutdownNow();
    server.shutdownNow();
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  private static void assertDeadlineExceeded(Runnable call) {
    long start = System.nanoTime();
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, call::run);
    assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
  }

  @Test
  public void testGetConnectedNodesTimesOut() {
    assertDeadlineExceeded(() -> client.getConnectedNodes("index"));
  }

  @Test
  public void testRecvCopyStateTimesOut() {
    assertDeadlineExceeded(() -> client.recvCopyState("index", "id", 1));
  }

  @Test
  public void testAddReplicasTimesOut() {
    long start = System.nanoTime();
    assertFalse(client.tryAddReplicas("index", "id", "node", "host", 1));
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
  }

  @Test
  public void testDefaultTimeout() {
    ReplicationServerClient defaultClient = new ReplicationServerClient(channel, "hung", 0, "");
    long start = System.nanoTime();
    assertThrows(StatusRuntimeException.class, () -> defaultClient.getConnectedNodes("index"));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(
        "elapsed " + elapsedMs,
        elapsedMs >= ReplicationServerClient.DEFAULT_CONTROL_CALL_TIMEOUT_MS - 500);
  }
}
