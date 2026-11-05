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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
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

    /**
     * Refresh a standby from remote global state: replace managers of recreated or removed indices,
     * reload changed index state, and make local started indices match remote state (start or stop
     * standby followers). Only called on a standby; the active primary is the source of global
     * state.
     *
     * @param abort checked between index loads, starts, and stops; when true, the refresh stops
     *     early so a pending role change is not delayed
     * @return true if fully applied, false if it stopped early or state changed while it ran (it is
     *     retried)
     */
    boolean refreshStandby(BooleanSupplier abort) throws IOException;

    /**
     * Stop, without changing global state, local indices that are started but not started in global
     * state, e.g. a standby follower of an index stopped on the previous leader.
     */
    void stopIndicesNotStarted() throws IOException;
  }

  // created by start(), once the local node's bound ports are known
  private final java.util.function.Supplier<PrimaryElector> electorFactory;
  private volatile PrimaryElector elector;
  private final EpochPointerFence fence;
  private final int claimConcurrency;
  private final ExecutorService transitionExecutor;
  private static final long CLOSE_WAIT_SECONDS = 60;
  // retries of a refresh dropped because state changed under it; afterwards the waiters wait for
  // the next periodic or requested refresh
  private static final int MAX_IMMEDIATE_REFRESH_RETRIES = 3;
  private final long refreshIntervalMs;
  private volatile ScheduledExecutorService refreshScheduler;
  // role changes queued but not yet started; a refresh stops early while any are pending
  private final AtomicInteger pendingTransitions = new AtomicInteger();
  // refresh requests not yet covered by a completed refresh, guarded by this. A refresh takes all
  // waiters when it starts, so a request made while a refresh runs is covered by a later one.
  private List<CompletableFuture<Void>> refreshWaiters = new ArrayList<>();
  // a refresh task is queued on the transition thread, guarded by this
  private boolean refreshTaskQueued = false;
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
    this(() -> elector, fence, claimConcurrency, controller, 0);
    Objects.requireNonNull(elector);
  }

  /**
   * Constructor.
   *
   * @param electorFactory creates the elector when election starts, after the server ports are
   *     bound, so the elector can advertise them
   * @param refreshIntervalMs interval between periodic state refreshes, or 0 to only refresh on
   *     request
   */
  public LeaderManager(
      java.util.function.Supplier<PrimaryElector> electorFactory,
      EpochPointerFence fence,
      int claimConcurrency,
      IndexRoleController controller,
      long refreshIntervalMs) {
    this.refreshIntervalMs = refreshIntervalMs;
    this.electorFactory = Objects.requireNonNull(electorFactory);
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

  /**
   * Start participating in election. Indices should already be started as standby. Must be called
   * once, after the server is listening, so the elector advertises the bound ports.
   */
  public void start() {
    PrimaryElector created;
    synchronized (this) {
      if (closed) {
        return;
      }
      if (elector != null) {
        throw new IllegalStateException("Leader manager already started");
      }
      created = Objects.requireNonNull(electorFactory.get());
      elector = created;
    }
    created.start(this);
    if (refreshIntervalMs > 0) {
      refreshScheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "LeaderManager-refresh");
                t.setDaemon(true);
                return t;
              });
      refreshScheduler.scheduleWithFixedDelay(
          this::requestRefresh, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }
  }

  public Role getRole() {
    return role;
  }

  /** If this node is the active primary, able to accept writes. */
  public boolean isActive() {
    PrimaryElector e = elector;
    return role == Role.ACTIVE && e != null && e.isLeader();
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
    PrimaryElector e = elector;
    return e == null ? Optional.empty() : e.currentLeader();
  }

  @Override
  public synchronized void onElected(LeaderInfo leaderInfo) {
    if (closed) {
      return;
    }
    long change = ++leadershipChangeCount;
    lossHandled = false;
    pendingTransitions.incrementAndGet();
    transitionExecutor.execute(
        () -> {
          pendingTransitions.decrementAndGet();
          promote(leaderInfo, change);
          drainRefreshes();
        });
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
    pendingTransitions.incrementAndGet();
    transitionExecutor.execute(
        () -> {
          pendingTransitions.decrementAndGet();
          demote("leadership lost at epoch " + leaderInfo.epoch(), change);
          drainRefreshes();
        });
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
    PrimaryElector e = elector;
    if (e != null) {
      e.resign(epoch);
    }
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
      // the active node must not keep followers of indices the previous leader stopped
      controller.stopIndicesNotStarted();
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
      // elector is non-null here: leadership changes only come from a started elector
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
    // pick up global state changes, and start any indices a failed promotion's reload left
    // unstarted. This only adds a waiter; the refresh runs (once, merged with any other requests)
    // when the transition thread drains refreshes after this demotion.
    requestRefreshInternal();
  }

  /** Add a refresh waiter without queueing a task; for use on the transition thread. */
  private synchronized void requestRefreshInternal() {
    if (!closed) {
      refreshWaiters.add(new CompletableFuture<>());
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

  /**
   * Request a refresh from remote global state, so a standby picks up created, recreated, or
   * removed indices. Does not block. Requests are merged: at most one refresh is queued at a time,
   * and it runs on the transition thread between role changes. A refresh stops early if a role
   * change is pending, and is run again after it. On the active primary, which is the source of
   * global state, a refresh does nothing.
   *
   * @return future completing when a refresh covering this request has finished
   */
  public CompletableFuture<Void> requestRefresh() {
    CompletableFuture<Void> done = new CompletableFuture<>();
    boolean queue;
    synchronized (this) {
      if (closed) {
        done.completeExceptionally(closedException());
        return done;
      }
      if (role == Role.ACTIVE && pendingTransitions.get() == 0) {
        // nothing to refresh on the active primary, the source of global state
        done.complete(null);
        return done;
      }
      refreshWaiters.add(done);
      queue = !refreshTaskQueued;
      refreshTaskQueued = true;
    }
    if (queue) {
      try {
        transitionExecutor.execute(this::drainRefreshes);
      } catch (RejectedExecutionException e) {
        // closed; close() completes the waiters
      }
    }
    return done;
  }

  /**
   * Run refreshes on the transition thread until no requests are waiting, or until a refresh can
   * not run now (a role change is pending; it runs this again afterwards). Waiters complete when a
   * refresh that started after their request finishes, exceptionally if it failed. On the active
   * primary, which is the source of global state, waiters complete at once.
   */
  private void drainRefreshes() {
    int immediateRetries = 0;
    while (true) {
      List<CompletableFuture<Void>> waiters;
      BooleanSupplier abort;
      synchronized (this) {
        refreshTaskQueued = false;
        if (refreshWaiters.isEmpty()) {
          return;
        }
        if (closed) {
          completeAll(takeWaiters(), closedException());
          return;
        }
        if (role == Role.ACTIVE) {
          completeAll(takeWaiters(), null);
          return;
        }
        if (role != Role.STANDBY || pendingTransitions.get() > 0) {
          // a role change is running or queued; it calls this again when done
          return;
        }
        waiters = takeWaiters();
        long change = leadershipChangeCount;
        abort =
            () ->
                isClosed()
                    || pendingTransitions.get() > 0
                    || change != leadershipChangeCount
                    || role != Role.STANDBY;
      }
      Throwable error = null;
      boolean finished = false;
      try {
        finished = controller.refreshStandby(abort);
      } catch (Throwable t) {
        error = t;
      }
      boolean retryNow = false;
      synchronized (this) {
        if (error != null) {
          logger.warn("State refresh failed", error);
          completeAll(waiters, error);
        } else if (finished) {
          completeAll(waiters, null);
        } else if (closed) {
          // close() already released waiters; do not strand these
          completeAll(waiters, closedException());
        } else {
          refreshWaiters.addAll(0, waiters);
          // stopped early: if a role change is pending it calls this again afterwards; otherwise
          // state changed underneath the refresh, retry now
          retryNow =
              role == Role.STANDBY
                  && pendingTransitions.get() == 0
                  && immediateRetries++ < MAX_IMMEDIATE_REFRESH_RETRIES;
        }
      }
      if (!finished && error == null && !retryNow) {
        return;
      }
    }
  }

  /**
   * Reported to refresh requests that were not run because the leader manager closed (server
   * shutdown). A distinct type, so callers do not mistake other failures for shutdown.
   */
  public static class ClosedException extends IllegalStateException {
    public ClosedException() {
      super("Leader manager is closed, state refresh not run");
    }
  }

  private static ClosedException closedException() {
    return new ClosedException();
  }

  /** Must hold the lock. */
  private List<CompletableFuture<Void>> takeWaiters() {
    List<CompletableFuture<Void>> waiters = refreshWaiters;
    refreshWaiters = new ArrayList<>();
    return waiters;
  }

  private static void completeAll(List<CompletableFuture<Void>> waiters, Throwable error) {
    for (CompletableFuture<Void> waiter : waiters) {
      if (error == null) {
        waiter.complete(null);
      } else {
        waiter.completeExceptionally(error);
      }
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
    ScheduledExecutorService scheduler = refreshScheduler;
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    synchronized (this) {
      // release any waiting reload requests; no refresh ran for them
      completeAll(takeWaiters(), closedException());
    }
    // stop election first, releasing leadership, so another node can take over while this node
    // waits below
    PrimaryElector started;
    synchronized (this) {
      started = elector;
    }
    if (started != null) {
      started.close();
    }
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
