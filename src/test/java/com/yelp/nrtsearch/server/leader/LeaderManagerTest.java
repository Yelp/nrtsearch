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
}
