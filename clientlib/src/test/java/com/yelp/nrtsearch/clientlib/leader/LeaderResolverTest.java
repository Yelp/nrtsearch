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

  /** Records what a tracker publishes. */
  static class RecordingSubscriber implements LeaderTracker.Subscriber {
    final List<LeaderResolver.Resolved> leaders = new ArrayList<>();
    int noLeader = 0;

    @Override
    public void onLeader(LeaderResolver.Resolved leader) {
      leaders.add(leader);
    }

    @Override
    public void onNoLeader() {
      noLeader++;
    }
  }

  private LeaderTracker tracker(List<Node> seeds) {
    return new LeaderTracker(resolver(seeds), 1000, () -> {});
  }

  private static void awaitIdle(LeaderTracker tracker) {
    try {
      tracker.awaitIdle();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testTrackerPublishesLeaderChangesOnly() {
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker = tracker(List.of(A, B));
    RecordingSubscriber sub = new RecordingSubscriber();
    tracker.subscribe(sub);
    awaitIdle(tracker);
    tracker.poll();
    tracker.poll();
    assertEquals(1, sub.leaders.size());
    assertEquals("a", sub.leaders.get(0).address().getHost());
    // failover to b
    answers.put(A, leader("b", 2, false));
    answers.put(B, leader("b", 2, true));
    tracker.poll();
    assertEquals(2, sub.leaders.size());
    assertEquals("b", sub.leaders.get(1).address().getHost());
    tracker.close();
  }

  @Test
  public void testTrackerKeepsLeaderWhenUnconfirmed() {
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker = tracker(List.of(A));
    RecordingSubscriber sub = new RecordingSubscriber();
    tracker.subscribe(sub);
    awaitIdle(tracker);
    tracker.poll();
    answers.clear();
    tracker.poll();
    assertEquals(1, sub.leaders.size());
    assertEquals(0, sub.noLeader);
    assertEquals(1, tracker.getCurrent().get().epoch());
    tracker.close();
  }

  @Test
  public void testTrackerNeverMovesToOlderEpoch() {
    answers.put(B, leader("b", 5, true));
    LeaderTracker tracker = tracker(List.of(A, B));
    RecordingSubscriber sub = new RecordingSubscriber();
    tracker.subscribe(sub);
    awaitIdle(tracker);
    tracker.poll();
    answers.clear();
    answers.put(A, leader("a", 4, true));
    tracker.poll();
    assertEquals(1, sub.leaders.size());
    assertEquals(5, tracker.getCurrent().get().epoch());
    tracker.close();
  }

  @Test
  public void testTrackerNoLeaderBeforeFirst() {
    LeaderTracker tracker = tracker(List.of(A));
    RecordingSubscriber sub = new RecordingSubscriber();
    tracker.subscribe(sub);
    awaitIdle(tracker);
    tracker.poll();
    assertEquals(1, sub.noLeader);
    tracker.close();
  }

  /** Many subscribers (e.g. one channel per replica index) share one tracker's resolution. */
  @Test
  public void testTrackerSharedBySubscribers() {
    answers.put(A, leader("a", 1, true));
    java.util.concurrent.atomic.AtomicInteger probes =
        new java.util.concurrent.atomic.AtomicInteger();
    LeaderTracker tracker =
        new LeaderTracker(
            new LeaderResolver(
                () -> List.of(A),
                node -> {
                  probes.incrementAndGet();
                  return answers.get(node);
                },
                LeaderResolver::clientAddress),
            1000,
            () -> {});
    List<RecordingSubscriber> subs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RecordingSubscriber sub = new RecordingSubscriber();
      subs.add(sub);
      tracker.subscribe(sub);
    }
    awaitIdle(tracker);
    tracker.poll();
    assertEquals(1, probes.get());
    for (RecordingSubscriber sub : subs) {
      assertEquals(1, sub.leaders.size());
    }
    // a late subscriber is told the current leader, on the tracker thread
    RecordingSubscriber late = new RecordingSubscriber();
    tracker.subscribe(late);
    awaitIdle(tracker);
    assertEquals(1, late.leaders.size());
    tracker.close();
  }

  /** The name resolver relays tracker results to its gRPC listener. */
  @Test
  public void testNameResolverRelaysToListener() {
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker = tracker(List.of(A));
    LeaderNameResolver nameResolver = new LeaderNameResolver(tracker, false, "");
    List<List<EquivalentAddressGroup>> results = new ArrayList<>();
    List<Status> errors = new ArrayList<>();
    nameResolver.start(
        new NameResolver.Listener2() {
          @Override
          public void onResult(NameResolver.ResolutionResult result) {
            results.add(result.getAddresses());
          }

          @Override
          public void onError(Status error) {
            errors.add(error);
          }
        });
    tracker.poll();
    assertEquals(1, results.size());
    assertEquals(
        "a", ((InetSocketAddress) results.get(0).get(0).getAddresses().get(0)).getHostString());
    // not owner: shutting down the name resolver leaves the shared tracker running
    nameResolver.shutdown();
    assertTrue(tracker.getCurrent().isPresent());
    tracker.close();
  }

  /** Closing an owned name resolver closes its tracker. */
  @Test
  public void testOwnedTrackerClosedWithResolver() {
    java.util.concurrent.atomic.AtomicBoolean closed =
        new java.util.concurrent.atomic.AtomicBoolean();
    LeaderTracker tracker = new LeaderTracker(resolver(List.of(A)), 1000, () -> closed.set(true));
    LeaderNameResolver nameResolver = new LeaderNameResolver(tracker, true, "");
    nameResolver.start(
        new NameResolver.Listener2() {
          @Override
          public void onResult(NameResolver.ResolutionResult result) {}

          @Override
          public void onError(Status error) {}
        });
    nameResolver.shutdown();
    assertTrue(closed.get());
  }

  @Test
  public void testRetainIgnoresEmptySeeds() {
    ProbeChannels channels = new ProbeChannels();
    channels.get(A);
    channels.retain(List.of());
    assertEquals(1, channels.size());
    channels.close();
  }

  /** A shut down probe executor does not fail resolution with an exception. */
  @Test
  public void testRejectedProbeExecutor() {
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    executor.shutdownNow();
    LeaderResolver resolver =
        new LeaderResolver(
            () -> List.of(A, B),
            node -> answers.get(node),
            LeaderResolver::clientAddress,
            executor,
            (seeds, candidate) -> {});
    assertFalse(resolver.resolve().isPresent());
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

  /**
   * A subscriber added while a poll publishes a newer leader never ends up on the older one:
   * deliveries are ordered on the tracker thread.
   */
  @Test
  public void testSubscribeRacingPollNeverEndsOnOlderLeader() throws Exception {
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker = tracker(List.of(A, B));
    tracker.start();
    long deadline = System.currentTimeMillis() + 5000;
    while (!tracker.getCurrent().isPresent() && System.currentTimeMillis() < deadline) {
      Thread.sleep(5);
    }
    for (int i = 0; i < 200; i++) {
      // failover to b at a higher epoch, racing a new subscriber
      long epoch = 2 + i;
      answers.put(A, leader("b", epoch, false));
      answers.put(B, leader("b", epoch, true));
      RecordingSubscriber sub = new RecordingSubscriber();
      tracker.subscribe(sub);
      tracker.refresh();
      awaitIdle(tracker);
      awaitIdle(tracker);
      List<LeaderResolver.Resolved> seen;
      synchronized (sub) {
        seen = new ArrayList<>(sub.leaders);
      }
      // epochs delivered to this subscriber never go down, and it ends on the newest
      for (int k = 1; k < seen.size(); k++) {
        assertTrue(seen.get(k).epoch() >= seen.get(k - 1).epoch());
      }
      assertEquals(epoch, seen.get(seen.size() - 1).epoch());
      tracker.unsubscribe(sub);
    }
    tracker.close();
  }

  /** Refresh requests from many channels at once are merged into a few polls. */
  @Test
  public void testRefreshCoalesced() throws Exception {
    java.util.concurrent.atomic.AtomicInteger probes =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker =
        new LeaderTracker(
            new LeaderResolver(
                () -> List.of(A),
                node -> {
                  probes.incrementAndGet();
                  release.await();
                  return answers.get(node);
                },
                LeaderResolver::clientAddress),
            1000,
            () -> {});
    for (int i = 0; i < 100; i++) {
      tracker.refresh();
    }
    release.countDown();
    awaitIdle(tracker);
    awaitIdle(tracker);
    // one poll for the first request, at most one more for all requests made while it ran
    assertTrue("probes " + probes.get(), probes.get() <= 2);
    tracker.close();
  }

  @Test
  public void testStartIdempotent() throws Exception {
    java.util.concurrent.atomic.AtomicInteger probes =
        new java.util.concurrent.atomic.AtomicInteger();
    answers.put(A, leader("a", 1, true));
    LeaderTracker tracker =
        new LeaderTracker(
            new LeaderResolver(
                () -> List.of(A),
                node -> {
                  probes.incrementAndGet();
                  return answers.get(node);
                },
                LeaderResolver::clientAddress),
            60_000,
            () -> {});
    tracker.start();
    tracker.start();
    awaitIdle(tracker);
    assertEquals(1, probes.get());
    tracker.close();
  }
}
