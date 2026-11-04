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
  private static final long CLOSE_WAIT_SECONDS = 60;
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
    long change;
    synchronized (this) {
      // a write from an earlier epoch may be fenced by this node's own newer claim; only a write
      // in the epoch currently held means another node took over
      long current = fence.getEpoch();
      if (e.getEpoch() <= 0 || e.getEpoch() != current) {
        logger.info(
            "Ignoring fenced write from epoch {}, current epoch is {}", e.getEpoch(), current);
        return;
      }
      change = leadershipChangeCount;
    }
    logger.warn("Remote state write fenced, resigning leadership", e);
    resignAndDemote(e.getEpoch(), change);
  }

  /**
   * Stop acting as leader: demote now, then resign from the elector. The elector may also report
   * the loss through {@link #onLost}, possibly asynchronously; only the first report demotes. Does
   * nothing if leadership changed since {@code change}, so a stale failure never gives up newer
   * leadership.
   *
   * @param epoch epoch being given up
   * @param change leadership change count the caller observed
   */
  private void resignAndDemote(long epoch, long change) {
    synchronized (this) {
      if (change != leadershipChangeCount) {
        logger.info("Not resigning for stale epoch {}, leadership has changed", epoch);
        return;
      }
      onLost(new LeaderInfo("", "", "", 0, 0, epoch));
    }
    // only gives up this epoch; if a newer epoch was acquired meanwhile it is kept
    elector.resign(epoch);
  }

  private void promote(LeaderInfo leaderInfo, long change) {
    if (isClosed() || change != leadershipChangeCount) {
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
      logger.error("Failed to promote at epoch {}", epoch, t);
      resignAndDemote(epoch, change);
    }
  }

  private synchronized boolean isClosed() {
    return closed;
  }

  private void checkStillLeader(long change, long epoch) throws IOException {
    if (isClosed() || change != leadershipChangeCount || !elector.isLeader()) {
      throw new FencedException("Leadership changed during promotion at epoch " + epoch);
    }
  }

  private void demote(String reason, long change) {
    if (isClosed() || change != leadershipChangeCount) {
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
      // no further remote state writes; also stops replica notifications, which are only sent after
      // a successful (fenced) upload. Indices are not restarted, they are closed by shutdown.
      fence.deactivate();
      if (role != Role.STANDBY) {
        role = Role.DEMOTING;
      }
      activeEpoch = -1;
      leadershipChangeCount++;
    }
    // stop election first, releasing leadership, so another node can take over while this node
    // waits below
    elector.close();
    // let a running transition finish, so it does not leave index managers started that nothing
    // will close. Queued transitions still run, but return immediately since closed is set.
    // Interrupt only as a last resort, since an interrupt during NIO closes the channel.
    transitionExecutor.shutdown();
    try {
      if (!transitionExecutor.awaitTermination(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)) {
        logger.warn("Leader transition did not finish in {}s, interrupting", CLOSE_WAIT_SECONDS);
        transitionExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      transitionExecutor.shutdownNow();
    }
  }
}
