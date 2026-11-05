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
package com.yelp.nrtsearch.clientlib.leader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class LeaderResolverTest {
  private static final Node A = new Node("a", 1);
  private static final Node B = new Node("b", 1);
  private static final Node C = new Node("c", 1);

  /** Fake cluster: what each node answers to resolveLeader. */
  private final Map<Node, ResolveLeaderResponse> answers = new HashMap<>();

  private static ResolveLeaderResponse leader(String host, long epoch, boolean isLeader) {
    return ResolveLeaderResponse.newBuilder()
        .setHost(host)
        .setPort(1)
        .setReplicationPort(2)
        .setNodeName(host)
        .setEpoch(epoch)
        .setIsLeader(isLeader)
        .build();
  }

  private LeaderResolver resolver(List<Node> seeds) {
    return new LeaderResolver(
        () -> seeds,
        node -> {
          ResolveLeaderResponse r = answers.get(node);
          if (r == null) {
            throw new RuntimeException("unreachable " + node);
          }
          return r;
        },
        LeaderResolver::clientAddress);
  }

  @Test
  public void testHighestEpochConfirmed() {
    // a missed a failover and still points at itself at epoch 5; b is leader at 6
    answers.put(A, leader("a", 5, false));
    answers.put(B, leader("b", 6, true));
    answers.put(C, leader("b", 6, false));
    Optional<LeaderResolver.Resolved> r = resolver(List.of(A, B, C)).resolve();
    assertEquals(new LeaderResolver.Resolved(B, 6), r.get());
  }

  @Test
  public void testUnconfirmedCandidateRejected() {
    // seeds believe b leads at 6, but b reports it is not leader (deposed)
    answers.put(A, leader("b", 6, false));
    answers.put(B, leader("b", 6, false));
    assertFalse(resolver(List.of(A, B)).resolve().isPresent());
  }

  @Test
  public void testCandidateAtDifferentEpochRejected() {
    answers.put(A, leader("b", 6, false));
    answers.put(B, leader("b", 7, true));
    // highest seed answer is b@7 (from b itself), confirmed by b
    assertEquals(new LeaderResolver.Resolved(B, 7), resolver(List.of(A, B)).resolve().get());
    // only a is a seed and reports b@6, but b is now at 7: not confirmed at 6
    assertFalse(resolver(List.of(A)).resolve().isPresent());
  }

  @Test
  public void testUnreachableSeedsSkipped() {
    answers.put(B, leader("b", 3, true));
    assertEquals(new LeaderResolver.Resolved(B, 3), resolver(List.of(A, B, C)).resolve().get());
  }

  @Test
  public void testNoSeedsReachable() {
    assertFalse(resolver(List.of(A, B)).resolve().isPresent());
  }

  /** Records what the name resolver publishes. */
  static class RecordingListener extends NameResolver.Listener2 {
    final List<List<EquivalentAddressGroup>> results = new ArrayList<>();
    final List<Status> errors = new ArrayList<>();

    @Override
    public void onResult(NameResolver.ResolutionResult result) {
      results.add(result.getAddresses());
    }

    @Override
    public void onError(Status error) {
      errors.add(error);
    }
  }

  private static InetSocketAddress address(List<EquivalentAddressGroup> groups) {
    return (InetSocketAddress) groups.get(0).getAddresses().get(0);
  }

  @Test
  public void testNameResolverPublishesLeaderChangesOnly() {
    answers.put(A, leader("a", 1, true));
    LeaderNameResolver nameResolver = new LeaderNameResolver(resolver(List.of(A, B)), 1000, "");
    RecordingListener listener = new RecordingListener();
    // drive polls manually; start() would schedule them
    setListener(nameResolver, listener);
    nameResolver.poll();
    nameResolver.poll();
    assertEquals(1, listener.results.size());
    assertEquals("a", address(listener.results.get(0)).getHostString());

    // failover to b
    answers.put(A, leader("b", 2, false));
    answers.put(B, leader("b", 2, true));
    nameResolver.poll();
    assertEquals(2, listener.results.size());
    assertEquals("b", address(listener.results.get(1)).getHostString());
  }

  @Test
  public void testNameResolverKeepsLeaderWhenUnconfirmed() {
    answers.put(A, leader("a", 1, true));
    LeaderNameResolver nameResolver = new LeaderNameResolver(resolver(List.of(A)), 1000, "");
    RecordingListener listener = new RecordingListener();
    setListener(nameResolver, listener);
    nameResolver.poll();
    // a becomes unreachable; no leader confirmed: keep a, publish nothing, no error
    answers.clear();
    nameResolver.poll();
    assertEquals(1, listener.results.size());
    assertTrue(listener.errors.isEmpty());
    assertEquals(1, nameResolver.getCurrent().get().epoch());
  }

  @Test
  public void testNameResolverNeverMovesToOlderEpoch() {
    answers.put(B, leader("b", 5, true));
    LeaderNameResolver nameResolver = new LeaderNameResolver(resolver(List.of(A, B)), 1000, "");
    RecordingListener listener = new RecordingListener();
    setListener(nameResolver, listener);
    nameResolver.poll();
    answers.clear();
    answers.put(A, leader("a", 4, true));
    nameResolver.poll();
    assertEquals(1, listener.results.size());
    assertEquals(5, nameResolver.getCurrent().get().epoch());
  }

  @Test
  public void testNameResolverErrorBeforeFirstLeader() {
    LeaderNameResolver nameResolver = new LeaderNameResolver(resolver(List.of(A)), 1000, "");
    RecordingListener listener = new RecordingListener();
    setListener(nameResolver, listener);
    nameResolver.poll();
    assertEquals(1, listener.errors.size());
    assertEquals(Status.Code.UNAVAILABLE, listener.errors.get(0).getCode());
  }

  private static void setListener(LeaderNameResolver resolver, NameResolver.Listener2 listener) {
    try {
      java.lang.reflect.Field f = LeaderNameResolver.class.getDeclaredField("listener");
      f.setAccessible(true);
      f.set(resolver, listener);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  private static Attributes unused() {
    return Attributes.EMPTY;
  }

  /** When the highest epoch answer is the leader's own, no separate confirm call is made. */
  @Test
  public void testConfirmSkippedWhenSeedIsLeader() {
    answers.put(A, leader("a", 3, true));
    java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    LeaderResolver resolver =
        new LeaderResolver(
            () -> List.of(A),
            node -> {
              calls.incrementAndGet();
              return answers.get(node);
            },
            LeaderResolver::clientAddress);
    assertEquals(new LeaderResolver.Resolved(A, 3), resolver.resolve().get());
    assertEquals(1, calls.get());
  }

  /** Seeds are probed in parallel, so slow seeds do not add up. */
  @Test
  public void testSeedsProbedInParallel() throws Exception {
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newCachedThreadPool();
    try {
      answers.put(C, leader("c", 1, true));
      LeaderResolver resolver =
          new LeaderResolver(
              () -> List.of(A, B, C),
              node -> {
                if (!node.equals(C)) {
                  Thread.sleep(500);
                  throw new RuntimeException("timeout");
                }
                return answers.get(node);
              },
              LeaderResolver::clientAddress,
              executor,
              (seeds, candidate) -> {});
      long start = System.nanoTime();
      assertEquals(new LeaderResolver.Resolved(C, 1), resolver.resolve().get());
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue("took " + elapsedMs, elapsedMs < 900);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Probe channels are closed for nodes no longer seeds or the leader, and not created after close.
   */
  @Test
  public void testProbeChannelsPrunedAndClosed() {
    ProbeChannels channels = new ProbeChannels();
    channels.get(A);
    channels.get(B);
    channels.get(C);
    assertEquals(3, channels.size());
    channels.retain(List.of(A, B));
    assertEquals(2, channels.size());
    channels.close();
    assertEquals(0, channels.size());
    try {
      channels.get(A);
      org.junit.Assert.fail("expected closed");
    } catch (IllegalStateException expected) {
      // a poll racing shutdown can not leak a new channel
    }
    assertEquals(0, channels.size());
  }

  /** The grpc resolver prunes channels to the current seeds plus the candidate. */
  @Test
  public void testGrpcResolverPrunesRemovedSeeds() {
    ProbeChannels channels = new ProbeChannels();
    java.util.concurrent.atomic.AtomicReference<List<Node>> seeds =
        new java.util.concurrent.atomic.AtomicReference<>(List.of(A, B, C));
    LeaderResolver resolver =
        LeaderResolver.grpc(
            seeds::get,
            channels,
            channel -> {
              throw new RuntimeException("unreachable");
            },
            LeaderResolver::clientAddress,
            Runnable::run);
    resolver.resolve();
    assertEquals(3, channels.size());
    seeds.set(List.of(A));
    resolver.resolve();
    assertEquals(1, channels.size());
    channels.close();
  }
}
