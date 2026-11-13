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
  // guarded by this
  private final List<Future<?>> running = new ArrayList<>();
  private boolean active = false;
  private long activeEpoch = -1;

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
  public synchronized void onActive(long epoch) {
    if (active) {
      return;
    }
    active = true;
    activeEpoch = epoch;
    logger.info("Starting ingestion for leadership epoch {}", epoch);
    for (Ingestor ingestor : ingestors) {
      // start may block for the whole term (e.g. a coordinator loop), so it runs on its own thread
      running.add(
          runner.submit(
              () -> {
                try {
                  ingestor.start();
                } catch (Throwable t) {
                  logger.error(
                      "Ingestor {} failed at epoch {}", ingestor.getClass().getName(), epoch, t);
                }
              }));
    }
  }

  @Override
  public synchronized void onInactive() {
    if (!active) {
      return;
    }
    active = false;
    logger.info("Stopping ingestion for leadership epoch {}", activeEpoch);
    for (Ingestor ingestor : ingestors) {
      try {
        ingestor.stop();
      } catch (Throwable t) {
        logger.warn("Error stopping ingestor {}", ingestor.getClass().getName(), t);
      }
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(stopWaitMs);
    for (Future<?> f : running) {
      try {
        f.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        logger.warn("Ingestor did not stop within {}ms, interrupting", stopWaitMs);
        f.cancel(true);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        f.cancel(true);
      } catch (Exception e) {
        // failure already logged by the runner
      }
    }
    running.clear();
  }

  /** If ingestion is running (this node is the active primary), for tests. */
  public synchronized boolean isActive() {
    return active;
  }
}
