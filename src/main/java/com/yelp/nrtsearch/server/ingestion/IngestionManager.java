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
package com.yelp.nrtsearch.server.ingestion;

import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.server.plugins.IngestionPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs ingestion plugins only on the active primary of a multi-primary cluster. Each ingestor is
 * started when this node becomes the active primary, and stopped when it stops being it (before its
 * indices are restarted as standby) or shuts down, so ingestion follows leadership across
 * failovers.
 *
 * <p>Ingestors must support this ({@link Ingestor#supportsLeaderElection()}), since they are
 * started and stopped once per leadership term, and must resume from the checkpoint durable in the
 * index. Data loss is prevented by fencing, not by the timing of start and stop: a stale primary's
 * commits fail, so neither its data nor its checkpoint become durable.
 *
 * <p>Without leader election, ingestion plugins are started once at server start, as before.
 */
public class IngestionManager implements LeaderManager.ActiveListener {
  private static final Logger logger = LoggerFactory.getLogger(IngestionManager.class);
  static final long STOP_WAIT_MS = 60_000;

  private final List<Ingestor> ingestors;
  private final ExecutorService runner =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "IngestionManager-runner");
            t.setDaemon(true);
            return t;
          });
  private final long stopWaitMs;
  // ingestion of the current leadership term, or null; guarded by this
  private Term term;
  // runners that did not finish within the stop wait, e.g. an ingest loop ignoring stop()
  private final java.util.concurrent.atomic.AtomicInteger leakedRunners =
      new java.util.concurrent.atomic.AtomicInteger();

  /** Ingestion of one leadership term. */
  private static final class Term {
    final long epoch;
    // guarded by this: set when the term ends; a start not yet begun is then skipped
    boolean stopped = false;
    // guarded by this: which ingestors' start() has begun
    final boolean[] entered;
    final List<Future<?>> starts = new ArrayList<>();

    Term(long epoch, int ingestors) {
      this.epoch = epoch;
      this.entered = new boolean[ingestors];
    }
  }

  IngestionManager(List<Ingestor> ingestors, long stopWaitMs) {
    this.ingestors = List.copyOf(ingestors);
    this.stopWaitMs = stopWaitMs;
  }

  /**
   * Set up ingestion plugins. Without leader election, each is initialized and started now. With
   * leader election, each is initialized now and run by an {@link IngestionManager} while this node
   * is the active primary.
   *
   * @param globalState global state
   * @param plugins loaded plugins
   * @return the manager, if leader election is enabled and there are ingestion plugins
   * @throws IllegalStateException if leader election is enabled and an ingestor does not support it
   */
  public static Optional<IngestionManager> initialize(GlobalState globalState, List<Plugin> plugins)
      throws IOException {
    List<IngestionPlugin> ingestionPlugins = new ArrayList<>();
    for (Plugin plugin : plugins) {
      if (plugin instanceof IngestionPlugin ingestionPlugin) {
        ingestionPlugins.add(ingestionPlugin);
      }
    }
    if (ingestionPlugins.isEmpty()) {
      return Optional.empty();
    }
    Optional<LeaderManager> leaderManager = globalState.getLeaderManager();
    if (leaderManager.isEmpty()) {
      for (IngestionPlugin plugin : ingestionPlugins) {
        IngestionPluginUtils.initializeAndStart(plugin, globalState);
      }
      return Optional.empty();
    }
    List<Ingestor> ingestors = new ArrayList<>();
    for (IngestionPlugin plugin : ingestionPlugins) {
      Ingestor ingestor = plugin.getIngestor();
      if (!ingestor.supportsLeaderElection()) {
        throw new IllegalStateException(
            "Ingestion plugin "
                + plugin.getClass().getName()
                + " does not support leader election (Ingestor.supportsLeaderElection), it can not"
                + " be used with leaderElection enabled");
      }
      ingestor.initialize(globalState);
      ingestors.add(ingestor);
    }
    IngestionManager manager = new IngestionManager(ingestors, STOP_WAIT_MS);
    leaderManager.get().addActiveListener(manager);
    logger.info("Ingestion will run while this node is the active primary");
    return Optional.of(manager);
  }

  @Override
  public void onActive(long epoch) {
    Term t;
    synchronized (this) {
      if (term != null) {
        return;
      }
      t = new Term(epoch, ingestors.size());
      term = t;
    }
    logger.info("Starting ingestion for leadership epoch {}", epoch);
    for (int i = 0; i < ingestors.size(); i++) {
      Ingestor ingestor = ingestors.get(i);
      int index = i;
      // start may block for the whole term (e.g. a coordinator loop), so it runs on its own thread
      t.starts.add(
          runner.submit(
              () -> {
                synchronized (t) {
                  if (t.stopped) {
                    // the term ended before this start began
                    return;
                  }
                  t.entered[index] = true;
                }
                try {
                  ingestor.start();
                } catch (Throwable e) {
                  logger.error(
                      "Ingestor {} failed at epoch {}", ingestor.getClass().getName(), epoch, e);
                }
              }));
    }
  }

  /**
   * Stop ingestion of the current term, bounded by the stop wait. Runs on the leader manager's
   * thread; stop() calls run on runner threads, so a hung plugin can not block role changes or
   * shutdown beyond the wait. An interrupt only ends the wait, it is not re-set, so it does not
   * abort the demotion that follows.
   */
  @Override
  public void onInactive() {
    Term t;
    synchronized (this) {
      t = term;
      term = null;
    }
    if (t == null) {
      return;
    }
    boolean[] entered;
    synchronized (t) {
      t.stopped = true;
      entered = t.entered.clone();
    }
    logger.info("Stopping ingestion for leadership epoch {}", t.epoch);
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(stopWaitMs);
    for (int i = 0; i < ingestors.size(); i++) {
      Future<?> start = t.starts.get(i);
      if (!entered[i]) {
        // never began, and never will now
        start.cancel(false);
        continue;
      }
      if (!stopIngestor(ingestors.get(i), start, deadline)) {
        int leaked = leakedRunners.incrementAndGet();
        logger.error(
            "Ingestor {} did not stop within {}ms at epoch {}; interrupting it. Ingestors must"
                + " return from start() and stop their threads promptly after stop(). Runners not"
                + " stopped so far: {}",
            ingestors.get(i).getClass().getName(),
            stopWaitMs,
            t.epoch,
            leaked);
        start.cancel(true);
      }
    }
  }

  /**
   * Stop one ingestor whose start() has begun. stop() may run before start() takes effect (e.g.
   * before it marks itself running), and then does nothing, so stop() is repeated until start()
   * returns, and called once more after that for ingestors whose start() returns while threads it
   * launched keep running.
   *
   * @return true if start() returned and stop() completed before the deadline
   */
  private boolean stopIngestor(Ingestor ingestor, Future<?> start, long deadline) {
    try {
      if (awaitDone(start, Math.min(TimeUnit.SECONDS.toNanos(1), remaining(deadline)))) {
        // start() returned (it launched its own threads): one stop() ends the run
        return callStop(ingestor, deadline);
      }
      // start() is still running: the ingest loop itself, or a slow start
      do {
        if (!callStop(ingestor, deadline)) {
          return false;
        }
      } while (!awaitDone(start, Math.min(TimeUnit.SECONDS.toNanos(1), remaining(deadline)))
          && remaining(deadline) > 0);
      // start() returned; if an earlier stop() ran before start() took effect, this one ends it
      return start.isDone() && callStop(ingestor, deadline);
    } catch (InterruptedException e) {
      logger.warn("Interrupted waiting for ingestor {} to stop", ingestor.getClass().getName());
      return false;
    }
  }

  /** Run stop() on a runner thread, waiting at most until the deadline. */
  private boolean callStop(Ingestor ingestor, long deadline) throws InterruptedException {
    Future<?> stop =
        runner.submit(
            () -> {
              try {
                ingestor.stop();
              } catch (Throwable e) {
                logger.warn("Error stopping ingestor {}", ingestor.getClass().getName(), e);
              }
            });
    return awaitDone(stop, remaining(deadline));
  }

  private static boolean awaitDone(Future<?> future, long timeoutNanos)
      throws InterruptedException {
    try {
      future.get(Math.max(0, timeoutNanos), TimeUnit.NANOSECONDS);
      return true;
    } catch (TimeoutException e) {
      return false;
    } catch (java.util.concurrent.CancellationException
        | java.util.concurrent.ExecutionException e) {
      return true;
    }
  }

  private static long remaining(long deadline) {
    return Math.max(0, deadline - System.nanoTime());
  }

  /** Runners that did not stop within the stop wait, for tests and diagnostics. */
  public int getLeakedRunners() {
    return leakedRunners.get();
  }

  /** If ingestion is running (this node is the active primary), for tests. */
  public synchronized boolean isActive() {
    return term != null;
  }
}
