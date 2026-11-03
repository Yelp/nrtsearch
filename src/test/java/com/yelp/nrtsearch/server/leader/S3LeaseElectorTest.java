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
package com.yelp.nrtsearch.server.leader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class S3LeaseElectorTest {
  private static final String KEY = "svc/leader/lease";
  private static final long TTL = 1000;
  private static final long MARGIN = 100;
  private static final long POLL = 300;

  private InMemoryConditionalObjectStore store;

  static class Clock {
    final AtomicLong nanos = new AtomicLong(1_000_000_000L);

    long now() {
      return nanos.get();
    }

    void advanceMs(long ms) {
      nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
    }
  }

  static class RecordingListener implements LeaderListener {
    final List<String> events = new ArrayList<>();

    @Override
    public synchronized void onElected(LeaderInfo leaderInfo) {
      events.add("elected:" + leaderInfo.epoch());
    }

    @Override
    public synchronized void onLost(LeaderInfo leaderInfo) {
      events.add("lost:" + leaderInfo.epoch());
    }

    synchronized List<String> get() {
      return new ArrayList<>(events);
    }
  }

  record Node(S3LeaseElector elector, RecordingListener listener, Clock clock) {
    void tick() throws Exception {
      elector.tick();
      elector.drainListener();
    }
  }

  /** Epoch of the first election event recorded by a node. */
  private static long e1(Node n) {
    String first = n.listener().get().get(0);
    return Long.parseLong(first.substring(first.indexOf(':') + 1));
  }

  @Before
  public void setUp() {
    store = new InMemoryConditionalObjectStore();
  }

  private Node node(String id) {
    Clock clock = new Clock();
    LeaderInfo info = new LeaderInfo(id, id + "-node", id + "-host", 1, 2, -1);
    S3LeaseElector elector = new S3LeaseElector(store, KEY, info, TTL, MARGIN, POLL, clock::now);
    RecordingListener listener = new RecordingListener();
    elector.startManual(listener);
    return new Node(elector, listener, clock);
  }

  @Test
  public void testFirstNodeAcquires() throws Exception {
    long start = System.currentTimeMillis();
    Node a = node("a");
    a.tick();
    assertTrue(a.elector().isLeader());
    // initial epoch is wall clock based, newer than legacy start time primaryGens
    long epoch = a.elector().getLeaderEpoch();
    assertTrue(epoch >= start);
    assertEquals(List.of("elected:" + epoch), a.listener().get());
    assertEquals("a", a.elector().currentLeader().get().leaderId());
  }

  @Test
  public void testSecondNodeObservesLeader() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    b.tick();
    assertFalse(b.elector().isLeader());
    assertEquals("a", b.elector().currentLeader().get().leaderId());
    assertEquals(a.elector().getLeaderEpoch(), b.elector().currentLeader().get().epoch());
  }

  @Test
  public void testRenewKeepsLeadership() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    b.tick();
    for (int i = 0; i < 10; i++) {
      a.clock().advanceMs(POLL);
      b.clock().advanceMs(POLL);
      a.tick();
      b.tick();
      assertTrue(a.elector().isLeader());
      assertFalse(b.elector().isLeader());
    }
    assertEquals(List.of("elected:" + e1(a)), a.listener().get());
    assertEquals(List.of(), b.listener().get());
  }

  @Test
  public void testTakeoverAfterExpiryIncrementsEpoch() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    long epochA = a.elector().getLeaderEpoch();
    b.tick();
    // a stops renewing; b must wait ttl + margin after first seeing this version
    b.clock().advanceMs(TTL);
    b.tick();
    assertFalse(b.elector().isLeader());
    b.clock().advanceMs(MARGIN + 1);
    b.tick();
    assertTrue(b.elector().isLeader());
    long epochB = b.elector().getLeaderEpoch();
    assertEquals(epochA + 1, epochB);
    assertEquals(List.of("elected:" + epochB), b.listener().get());
  }

  @Test
  public void testHolderStopsBeingLeaderBeforeTakeover() throws Exception {
    Node a = node("a");
    a.tick();
    // a's validity is ttl - margin from its renew send time
    a.clock().advanceMs(TTL - MARGIN - 1);
    assertTrue(a.elector().isLeader());
    a.clock().advanceMs(1);
    assertFalse(a.elector().isLeader());
  }

  @Test
  public void testPausedHolderLosesOnNextTick() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    b.tick();
    // a pauses (GC) past its validity, b takes over
    a.clock().advanceMs(5 * TTL);
    b.clock().advanceMs(TTL + MARGIN + 1);
    b.tick();
    assertTrue(b.elector().isLeader());
    // a wakes, must not renew stale lease
    a.tick();
    assertFalse(a.elector().isLeader());
    assertEquals(List.of("elected:" + e1(a), "lost:" + e1(a)), a.listener().get());
    assertTrue(new String(store.get(KEY).get().data()).contains("\"leaderId\":\"b\""));
  }

  @Test
  public void testRenewFailsIfLeaseChanged() throws Exception {
    Node a = node("a");
    a.tick();
    // another writer replaced the lease
    store.put(
        KEY, "{\"holder\":{\"leaderId\":\"x\",\"epoch\":5},\"ttlMs\":1000}".getBytes(), Map.of());
    a.clock().advanceMs(POLL);
    a.tick();
    assertFalse(a.elector().isLeader());
    assertEquals(List.of("elected:" + e1(a), "lost:" + e1(a)), a.listener().get());
  }

  @Test
  public void testResignAllowsImmediateTakeoverWithHigherEpoch() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    b.tick();
    a.elector().resign();
    a.elector().drainListener();
    assertFalse(a.elector().isLeader());
    assertEquals(List.of("elected:" + e1(a), "lost:" + e1(a)), a.listener().get());
    b.tick();
    assertTrue(b.elector().isLeader());
    assertEquals(e1(a) + 1, b.elector().getLeaderEpoch());
  }

  @Test
  public void testConcurrentAcquireOnlyOneWins() throws Exception {
    Node a = node("a");
    Node b = node("b");
    // both see no lease, a writes first; b's putIfAbsent fails
    a.tick();
    b.elector().tick();
    b.elector().drainListener();
    assertTrue(a.elector().isLeader());
    assertFalse(b.elector().isLeader());
  }

  @Test
  public void testEpochsMonotonicAcrossManyFailovers() throws Exception {
    Node a = node("a");
    Node b = node("b");
    a.tick();
    long lastEpoch = a.elector().getLeaderEpoch();
    Node leader = a;
    Node other = b;
    for (int i = 0; i < 5; i++) {
      other.tick();
      leader.elector().resign();
      leader.elector().drainListener();
      other.tick();
      assertTrue(other.elector().isLeader());
      long epoch = other.elector().getLeaderEpoch();
      assertTrue(epoch > lastEpoch);
      lastEpoch = epoch;
      Node t = leader;
      leader = other;
      other = t;
    }
  }

  @Test
  public void testInvalidParams() {
    LeaderInfo info = new LeaderInfo("a", "a", "h", 1, 2, -1);
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3LeaseElector(store, KEY, info, 1000, 500, 100, System::nanoTime));
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3LeaseElector(store, KEY, info, 1000, 100, 900, System::nanoTime));
  }
}
