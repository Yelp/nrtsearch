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

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives the role of this node in a multi-primary cluster, based on leadership from a {@link
 * PrimaryElector}.
 *
 * <ul>
 *   <li>STANDBY: indices follow index data from remote storage, the pointer fence rejects all
 *       remote state writes.
 *   <li>PROMOTING: the fence is activated with the new epoch, and every remote state pointer is
 *       claimed before indices are restarted as primary with primaryGen = epoch. Claiming makes all
 *       further pointer writes by any older leader fail.
 *   <li>ACTIVE: indices are primary.
 *   <li>DEMOTING: the fence is deactivated first, so no further remote state writes can happen,
 *       then indices are restarted as standby followers.
 * </ul>
 *
 * Role transitions run on a single thread, in order.
 */
public class LeaderManager implements Closeable, LeaderListener {
  private static final Logger logger = LoggerFactory.getLogger(LeaderManager.class);

  public enum Role {
    STANDBY,
    PROMOTING,
    ACTIVE,
    DEMOTING
  }

  /** Operations on the server state used by role transitions. */
  public interface IndexRoleController {
    /**
     * Claim the global state pointer for the active fence epoch, and reload global state.
     *
     * @throws FencedException if a newer leader owns the state
     */
    void claimAndReloadGlobalState() throws IOException;

    /** Names of indices that should be started. */
    Set<String> getIndicesToStart();

    /**
     * Claim the index state and point state pointers of an index for the active fence epoch.
     *
     * @throws FencedException if a newer leader owns the state
     */
    void claimIndex(String indexName) throws IOException;

    /** Restart an index as primary with the given primaryGen, or start it if not started. */
    void restartAsPrimary(String indexName, long primaryGen) throws IOException;

    /** Restart an index as a standby follower, or start it if not started. */
    void restartAsStandby(String indexName) throws IOException;
  }

  private final PrimaryElector elector;
  private final EpochPointerFence fence;
  private final int claimConcurrency;
  private final ExecutorService transitionExecutor;
  private final IndexRoleController controller;
  private volatile Role role = Role.STANDBY;
  private volatile long activeEpoch = -1;
  // incremented on every leadership change, lets queued transitions detect they are stale
  private volatile long leadershipChangeCount = 0;
  private boolean closed = false;
  // set when leadership is lost and a demotion is queued, cleared on election; ensures one
  // demotion per loss no matter how many sources report it (elector callback, fence, promotion)
  private boolean lossHandled = true;
  // true once any index may have been restarted as primary; cleared after restarting as standby.
  // only accessed on the transition thread
  private boolean indicesMayBePrimary = false;

  public LeaderManager(
      PrimaryElector elector,
      EpochPointerFence fence,
      int claimConcurrency,
      IndexRoleController controller) {
    this.elector = Objects.requireNonNull(elector);
    this.fence = Objects.requireNonNull(fence);
    this.controller = Objects.requireNonNull(controller);
    this.claimConcurrency = claimConcurrency;
    this.transitionExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "LeaderManager-transition");
              t.setDaemon(true);
              return t;
            });
    fence.registerFencedListener(this::onFenced);
  }

  /** Start participating in election. Indices should already be started as standby. */
  public void start() {
    elector.start(this);
  }

  public Role getRole() {
    return role;
  }

  /** If this node is the active primary, able to accept writes. */
  public boolean isActive() {
    return role == Role.ACTIVE && elector.isLeader();
  }

  /** Epoch of active leadership, or -1. */
  public long getActiveEpoch() {
    return activeEpoch;
  }

  public PrimaryElector getElector() {
    return elector;
  }

  public EpochPointerFence getFence() {
    return fence;
  }

  /** Last known leader of the cluster. */
  public Optional<LeaderInfo> currentLeader() {
    return elector.currentLeader();
  }

  @Override
  public synchronized void onElected(LeaderInfo leaderInfo) {
    if (closed) {
      return;
    }
    long change = ++leadershipChangeCount;
    lossHandled = false;
    transitionExecutor.execute(() -> promote(leaderInfo, change));
  }

  @Override
  public synchronized void onLost(LeaderInfo leaderInfo) {
    // stop all remote state writes immediately, before queueing the slower index restart
    fence.deactivate();
    if (closed || lossHandled) {
      return;
    }
    lossHandled = true;
    long change = ++leadershipChangeCount;
    if (role != Role.STANDBY) {
      role = Role.DEMOTING;
    }
    activeEpoch = -1;
    transitionExecutor.execute(
        () -> demote("leadership lost at epoch " + leaderInfo.epoch(), change));
  }

  private void onFenced(FencedException e) {
    logger.warn("Remote state write fenced, resigning leadership", e);
    resignAndDemote(activeEpoch);
  }

  /**
   * Stop acting as leader: demote now, then resign from the elector. The elector may also report
   * the loss through {@link #onLost}, possibly asynchronously; only the first report demotes.
   */
  private void resignAndDemote(long epoch) {
    onLost(new LeaderInfo("", "", "", 0, 0, epoch));
    elector.resign();
  }

  private void promote(LeaderInfo leaderInfo, long change) {
    if (change != leadershipChangeCount) {
      logger.info("Skipping stale promotion for epoch {}", leaderInfo.epoch());
      return;
    }
    long epoch = leaderInfo.epoch();
    logger.info("Promoting to active primary at epoch {}", epoch);
    synchronized (this) {
      if (change != leadershipChangeCount) {
        logger.info("Skipping stale promotion for epoch {}", epoch);
        return;
      }
      role = Role.PROMOTING;
    }
    try {
      fence.activate(epoch);
      controller.claimAndReloadGlobalState();
      List<String> indices = new ArrayList<>(controller.getIndicesToStart());
      runParallel(indices, name -> controller.claimIndex(name));
      checkStillLeader(change, epoch);
      indicesMayBePrimary = true;
      runParallel(indices, name -> controller.restartAsPrimary(name, epoch));
      synchronized (this) {
        checkStillLeader(change, epoch);
        activeEpoch = epoch;
        role = Role.ACTIVE;
      }
      logger.info("Active primary at epoch {}, indices: {}", epoch, indices);
    } catch (Throwable t) {
      logger.error("Failed to promote at epoch {}, resigning", epoch, t);
      resignAndDemote(epoch);
    }
  }

  private void checkStillLeader(long change, long epoch) throws IOException {
    if (change != leadershipChangeCount || !elector.isLeader()) {
      throw new FencedException("Leadership changed during promotion at epoch " + epoch);
    }
  }

  private void demote(String reason, long change) {
    if (change != leadershipChangeCount) {
      logger.info("Skipping stale demotion: {}", reason);
      return;
    }
    if (!indicesMayBePrimary) {
      // promotion failed before any index restarted as primary, indices are still standby
      logger.info("Leadership lost before indices became primary, staying standby: {}", reason);
    } else {
      logger.warn("Demoting to standby: {}", reason);
      try {
        List<String> indices = new ArrayList<>(controller.getIndicesToStart());
        runParallel(indices, name -> controller.restartAsStandby(name));
        indicesMayBePrimary = false;
      } catch (Throwable t) {
        logger.error("Error restarting indices as standby", t);
      }
    }
    synchronized (this) {
      if (change == leadershipChangeCount) {
        role = Role.STANDBY;
      }
    }
  }

  @FunctionalInterface
  private interface IndexOp {
    void apply(String indexName) throws IOException;
  }

  private void runParallel(List<String> indices, IndexOp op) throws Exception {
    if (indices.isEmpty()) {
      return;
    }
    ExecutorService pool = Executors.newFixedThreadPool(Math.min(claimConcurrency, indices.size()));
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (String name : indices) {
        futures.add(
            pool.submit(
                () -> {
                  op.apply(name);
                  return null;
                }));
      }
      Exception first = null;
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (Exception e) {
          if (first == null) {
            first = e;
          } else {
            first.addSuppressed(e);
          }
        }
      }
      if (first != null) {
        throw first;
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** Wait for queued transitions to finish, for tests. */
  @VisibleForTesting
  public void awaitTransitions() throws Exception {
    transitionExecutor.submit(() -> {}).get(60, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      closed = true;
    }
    // stop transitions first, so resigning below does not restart indices during shutdown
    transitionExecutor.shutdownNow();
    fence.deactivate();
    elector.close();
  }
}
