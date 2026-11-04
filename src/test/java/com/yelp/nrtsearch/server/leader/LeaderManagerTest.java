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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Before;
import org.junit.Test;

public class LeaderManagerTest {
  private static final String KEY = "svc/global_state/_current";

  /** Elector controlled by the test; callbacks are synchronous. */
  static class TestElector implements PrimaryElector {
    LeaderListener listener;
    volatile LeaderInfo held;
    int resignCount = 0;
    boolean closed = false;

    @Override
    public void start(LeaderListener listener) {
      this.listener = listener;
    }

    void elect(long epoch) {
      held = new LeaderInfo("a", "a", "h", 1, 2, epoch);
      listener.onElected(held);
    }

    void lose() {
      LeaderInfo lost = held;
      held = null;
      listener.onLost(lost);
    }

    @Override
    public Optional<LeaderInfo> currentLeader() {
      return Optional.ofNullable(held);
    }

    @Override
    public boolean isLeader() {
      return held != null;
    }

    @Override
    public void resign() {
      resignCount++;
      if (held != null) {
        lose();
      }
    }

    @Override
    public void close() {
      closed = true;
      resign();
    }
  }

  static class RecordingController implements LeaderManager.IndexRoleController {
    final List<String> events = new CopyOnWriteArrayList<>();
    final EpochPointerFence fence;
    volatile boolean failClaim = false;
    volatile Runnable onClaim = () -> {};

    RecordingController(EpochPointerFence fence) {
      this.fence = fence;
    }

    @Override
    public void claimAndReloadGlobalState() throws IOException {
      events.add("claimGlobal");
      fence.claim(KEY);
    }

    @Override
    public Set<String> getIndicesToStart() {
      return Set.of("idx");
    }

    @Override
    public void claimIndex(String indexName) throws IOException {
      onClaim.run();
      if (failClaim) {
        throw new FencedException("newer leader");
      }
      events.add("claim:" + indexName);
    }

    @Override
    public void restartAsPrimary(String indexName, long primaryGen) {
      events.add("primary:" + indexName + ":" + primaryGen);
    }

    @Override
    public void restartAsStandby(String indexName) {
      events.add("standby:" + indexName);
    }

    List<String> restarts() {
      List<String> r = new ArrayList<>();
      for (String e : events) {
        if (e.startsWith("primary:") || e.startsWith("standby:")) {
          r.add(e);
        }
      }
      return r;
    }
  }

  private InMemoryConditionalObjectStore store;
  private EpochPointerFence fence;
  private TestElector elector;
  private RecordingController controller;
  private LeaderManager manager;

  @Before
  public void setUp() {
    store = new InMemoryConditionalObjectStore();
    fence = new EpochPointerFence(store);
    elector = new TestElector();
    controller = new RecordingController(fence);
    manager = new LeaderManager(elector, fence, 4, controller);
    manager.start();
  }

  @Test
  public void testPromoteAndDemote() throws Exception {
    elector.elect(10);
    manager.awaitTransitions();
    assertEquals(LeaderManager.Role.ACTIVE, manager.getRole());
    assertTrue(manager.isActive());
    assertEquals(10, fence.getEpoch());

    elector.lose();
    assertFalse(fence.isActive());
    manager.awaitTransitions();
    assertEquals(LeaderManager.Role.STANDBY, manager.getRole());
    assertEquals(List.of("primary:idx:10", "standby:idx"), controller.restarts());
  }

  /** A fenced write demotes once, even though the elector also reports the loss. */
  @Test
  public void testFencedWriteDemotesOnce() throws Exception {
    elector.elect(10);
    manager.awaitTransitions();
    fence.write(KEY, "v0");
    // another leader claims the pointer
    EpochPointerFence other = new EpochPointerFence(store);
    other.activate(11);
    other.claim(KEY);
    assertThrows(FencedException.class, () -> fence.write(KEY, "v"));
    manager.awaitTransitions();
    manager.awaitTransitions();
    assertEquals(LeaderManager.Role.STANDBY, manager.getRole());
    assertEquals(1, elector.resignCount);
    assertEquals(List.of("primary:idx:10", "standby:idx"), controller.restarts());
  }

  /** Promotion that fails before any index restarted does not restart indices as standby. */
  @Test
  public void testFailedPromotionDoesNotRestartIndices() throws Exception {
    controller.failClaim = true;
    elector.elect(10);
    manager.awaitTransitions();
    manager.awaitTransitions();
    assertEquals(LeaderManager.Role.STANDBY, manager.getRole());
    assertFalse(fence.isActive());
    assertEquals(List.of(), controller.restarts());
    assertEquals(1, elector.resignCount);
  }

  /** Shutdown releases leadership without restarting indices. */
  @Test
  public void testCloseReleasesLeadershipWithoutRestart() throws Exception {
    elector.elect(10);
    manager.awaitTransitions();
    manager.close();
    assertTrue(elector.closed);
    assertFalse(elector.isLeader());
    assertFalse(fence.isActive());
    assertEquals(List.of("primary:idx:10"), controller.restarts());
  }

  @Test
  public void testReelectionAfterLoss() throws Exception {
    elector.elect(10);
    manager.awaitTransitions();
    elector.lose();
    manager.awaitTransitions();
    elector.elect(12);
    manager.awaitTransitions();
    assertEquals(LeaderManager.Role.ACTIVE, manager.getRole());
    assertEquals(List.of("primary:idx:10", "standby:idx", "primary:idx:12"), controller.restarts());
  }

  /**
   * A promotion for an old epoch fails after this node already lost it and won a newer epoch. The
   * failure must not give up the newer leadership.
   */
  @Test
  public void testStalePromotionFailureKeepsNewerLeadership() throws Exception {
    java.util.concurrent.CountDownLatch inClaim = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch proceed = new java.util.concurrent.CountDownLatch(1);
    controller.onClaim =
        () -> {
          if (fence.getEpoch() == 10) {
            inClaim.countDown();
            try {
              proceed.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            controller.failClaim = true;
          } else {
            controller.failClaim = false;
          }
        };
    elector.elect(10);
    assertTrue(inClaim.await(5, java.util.concurrent.TimeUnit.SECONDS));
    // lose epoch 10 and win epoch 11 while promotion for 10 is still running
    elector.lose();
    elector.elect(11);
    int resignsBefore = elector.resignCount;
    proceed.countDown();
    manager.awaitTransitions();
    manager.awaitTransitions();
    assertEquals(resignsBefore, elector.resignCount);
    assertTrue(elector.isLeader());
    assertEquals(LeaderManager.Role.ACTIVE, manager.getRole());
    assertEquals(11, manager.getActiveEpoch());
  }

  /** A write from an earlier epoch, fenced by this node's own newer claim, must not resign. */
  @Test
  public void testOwnStaleWriteDoesNotResign() throws Exception {
    elector.elect(10);
    manager.awaitTransitions();
    fence.write(KEY, "v10");
    // re-elected at 11 (e.g. after a transient loss); 11 claims the pointer
    elector.lose();
    manager.awaitTransitions();
    elector.elect(11);
    manager.awaitTransitions();
    int resignsBefore = elector.resignCount;
    // a write that captured epoch 10 completes now: simulate by a fence at epoch 10 on same store
    EpochPointerFence stale = new EpochPointerFence(store);
    stale.activate(10);
    FencedException e = new FencedException("stale", 10);
    // deliver through the manager's listener path
    java.lang.reflect.Method m =
        LeaderManager.class.getDeclaredMethod("onFenced", FencedException.class);
    m.setAccessible(true);
    m.invoke(manager, e);
    manager.awaitTransitions();
    assertEquals(resignsBefore, elector.resignCount);
    assertEquals(LeaderManager.Role.ACTIVE, manager.getRole());
  }

  /** Close waits for a running promotion instead of abandoning it. */
  @Test
  public void testCloseWaitsForRunningTransition() throws Exception {
    java.util.concurrent.CountDownLatch inClaim = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.atomic.AtomicBoolean interrupted =
        new java.util.concurrent.atomic.AtomicBoolean();
    controller.onClaim =
        () -> {
          inClaim.countDown();
          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            interrupted.set(true);
          }
        };
    elector.elect(10);
    assertTrue(inClaim.await(5, java.util.concurrent.TimeUnit.SECONDS));
    manager.close();
    assertFalse(interrupted.get());
    // promotion saw the close and did not restart indices as primary
    assertEquals(List.of(), controller.restarts());
    assertFalse(fence.isActive());
  }
}
