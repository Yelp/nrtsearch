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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.ThreadPoolConfiguration;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.DummyResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexV2Request;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.utils.ThreadPoolExecutorFactory;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.lucene.search.TimeLimitingCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GlobalState implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(GlobalState.class);
  private final String hostName;
  private final int port;
  private final ThreadPoolConfiguration threadPoolConfiguration;
  private final Archiver incArchiver;
  private int replicaReplicationPortPingInterval;
  private final String ephemeralId = UUID.randomUUID().toString();
  private final long generation = System.currentTimeMillis();

  private final String nodeName;

  private final LuceneServerConfiguration configuration;

  /** Server shuts down once this latch is decremented. */
  private final CountDownLatch shutdownNow = new CountDownLatch(1);

  private final Path stateDir;
  private final Path indexDirBase;

  private final ExecutorService indexService;
  private final ExecutorService fetchService;
  private final ThreadPoolExecutor searchThreadPoolExecutor;

  public static GlobalState createState(LuceneServerConfiguration luceneServerConfiguration)
      throws IOException {
    return createState(luceneServerConfiguration, null);
  }

  public static GlobalState createState(
      LuceneServerConfiguration luceneServerConfiguration, Archiver incArchiver)
      throws IOException {
    return createState(luceneServerConfiguration, incArchiver, null);
  }

  public static GlobalState createState(
      LuceneServerConfiguration luceneServerConfiguration,
      Archiver incArchiver,
      Archiver legacyArchiver)
      throws IOException {
    return new BackendGlobalState(luceneServerConfiguration, incArchiver, legacyArchiver);
  }

  public Optional<Archiver> getIncArchiver() {
    return Optional.ofNullable(incArchiver);
  }

  protected GlobalState(LuceneServerConfiguration luceneServerConfiguration, Archiver incArchiver)
      throws IOException {
    this.incArchiver = incArchiver;
    this.nodeName = luceneServerConfiguration.getNodeName();
    this.stateDir = Paths.get(luceneServerConfiguration.getStateDir());
    this.indexDirBase = Paths.get(luceneServerConfiguration.getIndexDir());
    this.hostName = luceneServerConfiguration.getHostName();
    this.port = luceneServerConfiguration.getPort();
    this.replicaReplicationPortPingInterval =
        luceneServerConfiguration.getReplicaReplicationPortPingInterval();
    this.threadPoolConfiguration = luceneServerConfiguration.getThreadPoolConfiguration();
    if (Files.exists(stateDir) == false) {
      Files.createDirectories(stateDir);
    }
    this.indexService =
        ThreadPoolExecutorFactory.getThreadPoolExecutor(
            ThreadPoolExecutorFactory.ExecutorType.INDEX,
            luceneServerConfiguration.getThreadPoolConfiguration());
    this.searchThreadPoolExecutor =
        ThreadPoolExecutorFactory.getThreadPoolExecutor(
            ThreadPoolExecutorFactory.ExecutorType.SEARCH,
            luceneServerConfiguration.getThreadPoolConfiguration());
    this.fetchService =
        ThreadPoolExecutorFactory.getThreadPoolExecutor(
            ThreadPoolExecutorFactory.ExecutorType.FETCH,
            luceneServerConfiguration.getThreadPoolConfiguration());
    this.configuration = luceneServerConfiguration;
  }

  public LuceneServerConfiguration getConfiguration() {
    return configuration;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  public int getReplicaReplicationPortPingInterval() {
    return replicaReplicationPortPingInterval;
  }

  public void setReplicaReplicationPortPingInterval(int replicaReplicationPortPingInterval) {
    this.replicaReplicationPortPingInterval = replicaReplicationPortPingInterval;
  }

  public Path getStateDir() {
    return stateDir;
  }

  public CountDownLatch getShutdownLatch() {
    return shutdownNow;
  }

  @Override
  public void close() throws IOException {
    indexService.shutdown();
    TimeLimitingCollector.getGlobalTimerThread().stopTimer();
    try {
      TimeLimitingCollector.getGlobalTimerThread().join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  /** Get base directory for all index data. */
  public Path getIndexDirBase() {
    return indexDirBase;
  }

  /** Get index data directory for given index name. */
  public Path getIndexDir(String indexName) {
    return Paths.get(indexDirBase.toString(), indexName);
  }

  /**
   * Get port the replication grpc server is listening on. This may be different from the value
   * specified in the config file when using port 0 (auto select). In this case, the true port will
   * be passed to the {@link #replicationStarted(int)} hook.
   */
  public abstract int getReplicationPort();

  /**
   * Hook that is invoked during startup after the replication grpc server starts, but before the
   * client grpc server. Operations such as starting indices can be done here.
   *
   * @param replicationPort resolved port replication grpc server is listening on, may be different
   *     from config port if using 0 (auto select).
   * @throws IOException
   */
  public abstract void replicationStarted(int replicationPort) throws IOException;

  /** Get the data resource name for a given index. Used with incremental archiver functionality. */
  public abstract String getDataResourceForIndex(String indexName);

  public abstract Set<String> getIndexNames();

  /** Get names of all indices that should be in the started state. */
  public abstract Set<String> getIndicesToStart();

  /** Create a new index. */
  public abstract IndexState createIndex(String name) throws IOException;

  /** Create a new index based on the given create request. */
  public abstract IndexState createIndex(CreateIndexRequest createIndexRequest) throws IOException;

  public abstract IndexState getIndex(String name, boolean hasRestore) throws IOException;

  /** Get the {@link IndexState} by index name. */
  public abstract IndexState getIndex(String name) throws IOException;

  /**
   * Get the state manager for a given index.
   *
   * @param name index name
   * @return state manager
   * @throws IOException on error reading index data
   */
  public abstract IndexStateManager getIndexStateManager(String name) throws IOException;

  /**
   * Reload state from backend
   *
   * @return
   * @throws IOException
   */
  public abstract void reloadStateFromBackend() throws IOException;

  /** Remove the specified index. */
  public abstract void deleteIndex(String name) throws IOException;

  /**
   * Start a created index using the given {@link StartIndexRequest}.
   *
   * @param startIndexRequest start request
   * @return start response
   * @throws IOException
   */
  public abstract StartIndexResponse startIndex(StartIndexRequest startIndexRequest)
      throws IOException;

  /**
   * Start a created index using the given {@link StartIndexV2Request}.
   *
   * @param startIndexRequest start request
   * @return start response
   * @throws IOException
   */
  public abstract StartIndexResponse startIndexV2(StartIndexV2Request startIndexRequest)
      throws IOException;

  /**
   * Stop a created index using the given {@link StopIndexRequest}.
   *
   * @param stopIndexRequest stop request
   * @return stop response
   * @throws IOException
   */
  public abstract DummyResponse stopIndex(StopIndexRequest stopIndexRequest) throws IOException;

  public Future<Long> submitIndexingTask(Callable<Long> job) {
    return indexService.submit(job);
  }

  public ThreadPoolConfiguration getThreadPoolConfiguration() {
    return threadPoolConfiguration;
  }

  public ThreadPoolExecutor getSearchThreadPoolExecutor() {
    return searchThreadPoolExecutor;
  }

  public ExecutorService getFetchService() {
    return fetchService;
  }

  public String getEphemeralId() {
    return ephemeralId;
  }

  /**
   * Get ephemeral, monotonically increasing value to use to start a primary index.
   *
   * @return generation
   */
  public long getGeneration() {
    return generation;
  }
}
