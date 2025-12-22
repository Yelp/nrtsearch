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
package com.yelp.nrtsearch.server.nrt;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.config.IsolatedReplicaConfig;
import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.nrt.jobs.CopyJobManager;
import com.yelp.nrtsearch.server.nrt.jobs.GrpcCopyJobManager;
import com.yelp.nrtsearch.server.nrt.jobs.RemoteCopyJobManager;
import com.yelp.nrtsearch.server.nrt.jobs.SimpleCopyJob;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.replicator.nrt.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRTReplicaNode extends ReplicaNode {
  private static final long NRT_SYNC_WAIT_MS = 2000;
  private static final long NRT_CONNECT_WAIT_MS = 500;

  private final ReplicationServerClient primaryAddress;
  private final ReplicaDeleterManager replicaDeleterManager;
  private final CopyJobManager copyJobManager;
  private final String indexName;
  private final String indexId;
  private final String nodeName;
  private final boolean ackedCopy;
  private final boolean filterIncompatibleSegmentReaders;
  final NrtCopyThread nrtCopyThread;

  /* Just a wrapper class to hold our <hostName, port> pair so that we can send them to the Primary
   * on sendReplicas and it can build its channel over this pair */
  private final HostPort hostPort;
  private static final Logger logger = LoggerFactory.getLogger(NRTReplicaNode.class);

  public NRTReplicaNode(
      String indexName,
      String indexId,
      ReplicationServerClient primaryAddress,
      HostPort hostPort,
      String nodeName,
      Directory indexDir,
      SearcherFactory searcherFactory,
      IsolatedReplicaConfig isolatedReplicaConfig,
      NrtDataManager nrtDataManager,
      PrintStream printStream,
      boolean ackedCopy,
      boolean decInitialCommit,
      boolean filterIncompatibleSegmentReaders,
      int lowPriorityCopyPercentage)
      throws IOException {
    // the id is always 0, the nodeName is the identifier
    super(0, indexDir, searcherFactory, printStream);
    this.primaryAddress = primaryAddress;
    this.indexName = indexName;
    this.indexId = indexId;
    this.nodeName = nodeName;
    this.ackedCopy = ackedCopy;
    this.hostPort = hostPort;
    replicaDeleterManager = decInitialCommit ? new ReplicaDeleterManager(this) : null;
    this.filterIncompatibleSegmentReaders = filterIncompatibleSegmentReaders;

    if (isolatedReplicaConfig.isEnabled()) {
      if (hasPrimaryConnection()) {
        throw new IllegalArgumentException(
            "Cannot have both primary connection and isolated replica enabled");
      }
      copyJobManager =
          new RemoteCopyJobManager(
              isolatedReplicaConfig.getPollingIntervalSeconds(),
              nrtDataManager,
              this,
              isolatedReplicaConfig);
    } else {
      copyJobManager =
          new GrpcCopyJobManager(indexName, indexId, primaryAddress, ackedCopy, this, id);
    }
    // Handles fetching files from primary, on a new thread which receives files from primary
    nrtCopyThread = getNrtCopyThread(this, lowPriorityCopyPercentage);
    nrtCopyThread.setName("R" + id + ".copyJobs");
    nrtCopyThread.setDaemon(true);
    nrtCopyThread.start();
  }

  @VisibleForTesting
  static NrtCopyThread getNrtCopyThread(Node node, int lowPriorityCopyPercentage) {
    if (lowPriorityCopyPercentage > 0) {
      return new ProportionalCopyThread(node, lowPriorityCopyPercentage);
    } else {
      return new DefaultCopyThread(node);
    }
  }

  @VisibleForTesting
  CopyJobManager getCopyJobManager() {
    return copyJobManager;
  }

  private long getLastPrimaryGen() throws IOException {
    // detection logic from ReplicaNode
    String segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
    if (segmentsFileName == null) {
      return -1;
    }
    SegmentInfos infos = SegmentInfos.readCommit(dir, segmentsFileName);
    String s = infos.getUserData().get(PRIMARY_GEN_KEY);
    if (s == null) {
      return -1;
    } else {
      return Long.parseLong(s);
    }
  }

  /**
   * Get if this replica has a primary connection configured.
   *
   * @return true if this replica has a primary connection, false otherwise
   */
  public boolean hasPrimaryConnection() {
    return primaryAddress != null;
  }

  /**
   * Start this replica node using the last known primary generation, as detected from the last
   * index commit. Ensures a live primary is not needed to start node.
   *
   * @throws IOException on filesystem error
   */
  public void startWithLastPrimaryGen() throws IOException {
    start(getLastPrimaryGen());
  }

  @Override
  public synchronized void start(long primaryGen) throws IOException {
    super.start(primaryGen);
    if (replicaDeleterManager != null) {
      replicaDeleterManager.decReplicaInitialCommitFiles();
    }
    if (filterIncompatibleSegmentReaders) {
      // Swap in a SearcherManager that filters incompatible segment readers during refresh.
      // Updating the reference is not thread safe, but since this happens under the object lock
      // and before the shard has stared, nothing should access the manager before the swap.
      ReferenceManager<IndexSearcher> oldMgr = mgr;
      mgr = new FilteringSegmentInfosSearcherManager(getDirectory(), this, mgr, searcherFactory);
      oldMgr.close();
    }
    copyJobManager.start();
  }

  @Override
  protected CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    return copyJobManager.newCopyJob(reason, files, prevFiles, highPriority, onceDone);
  }

  @Override
  protected void launch(CopyJob job) {
    nrtCopyThread.launch(job);
  }

  /**
   * Called once start(primaryGen) is invoked on this object (see constructor). If no primary
   * connection is configured, this is a noop.
   */
  @Override
  protected void sendNewReplica() throws IOException {
    if (hasPrimaryConnection()) {
      logger.info(String.format("send new_replica to primary: %s", primaryAddress));
      primaryAddress.addReplicas(
          indexName, this.indexId, this.nodeName, hostPort.getHostName(), hostPort.getPort());
    }
  }

  public CopyJob launchPreCopyFiles(
      AtomicBoolean finished, long curPrimaryGen, Map<String, FileMetaData> files)
      throws IOException {
    return launchPreCopyMerge(finished, curPrimaryGen, files);
  }

  @Override
  protected void finishNRTCopy(CopyJob job, long startNS) throws IOException {
    super.finishNRTCopy(job, startNS);

    copyJobManager.finishNRTCopy(job);

    // record metrics for this nrt point
    if (job.getFailed()) {
      NrtMetrics.nrtPointFailure.labelValues(indexName).inc();
    } else {
      NrtMetrics.nrtPointTime
          .labelValues(indexName)
          .observe((System.nanoTime() - startNS) / 1000000.0);
      NrtMetrics.nrtPointSize.labelValues(indexName).observe(job.getTotalBytesCopied());
      NrtMetrics.searcherVersion.labelValues(indexName).set(job.getCopyState().version());

      // if the job is a simple copy job, read out the index data timestamp and update the metric
      if (job instanceof SimpleCopyJob simpleCopyJob) {
        Instant timestamp = simpleCopyJob.getTimestamp();
        if (timestamp != null) {
          NrtMetrics.indexTimestampSec.labelValues(indexName).set(timestamp.getEpochSecond());
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    nrtCopyThread.close();
    logger.info("CLOSE NRT REPLICA");
    copyJobManager.close();
    message("top: jobs closed");
    synchronized (mergeCopyJobs) {
      for (CopyJob job : mergeCopyJobs) {
        message("top: cancel merge copy job " + job);
        job.cancel("jobs closing", null);
      }
    }
    if (hasPrimaryConnection()) {
      primaryAddress.close();
    }
    super.close();
  }

  @VisibleForTesting
  public ReplicaDeleterManager getReplicaDeleterManager() {
    return replicaDeleterManager;
  }

  public ReplicationServerClient getPrimaryAddress() {
    return primaryAddress;
  }

  public String getNodeName() {
    return nodeName;
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  /**
   * Returns true if present in primary's current list of known replicas else false.
   *
   * <p>Throws StatusRuntimeException if cannot reach Primary
   */
  public boolean isKnownToPrimary() {
    if (!hasPrimaryConnection()) {
      return false;
    }
    GetNodesResponse getNodesResponse = primaryAddress.getConnectedNodes(indexName);
    for (NodeInfo nodeInfo : getNodesResponse.getNodesList()) {
      if (nodeName.equals(nodeInfo.getNodeName())
          && hostPort.equals(new HostPort(nodeInfo.getHostname(), nodeInfo.getPort()))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Sync the next nrt point from the current primary. Attempts to get the current index version
   * from the primary, giving up after the specified amount of time. Sync is considered completed
   * when either the index version has updated to at least the initial primary version, there is a
   * failure to start a new copy job, or the specified max time elapses.
   *
   * <p>If this node is not connected to a primary, this method will return immediately
   *
   * @param primaryWaitMs how long to wait for primary to be available
   * @param maxTimeMs max time to attempt initial point sync
   * @throws IOException on issue getting searcher version
   */
  public void syncFromCurrentPrimary(long primaryWaitMs, long maxTimeMs) throws IOException {
    if (!hasPrimaryConnection()) {
      logger.info("Skipping primary sync, no primary connection available");
      return;
    }
    logger.info("Starting sync of next nrt point from current primary");
    long startMS = System.currentTimeMillis();
    long primaryIndexVersion = -1;
    long lastPrimaryGen = -1;
    // Attempt to get the current index version from the primary, give up if the timeout
    // is exceeded
    while (System.currentTimeMillis() - startMS < primaryWaitMs) {
      try {
        com.yelp.nrtsearch.server.grpc.CopyState copyState =
            primaryAddress.recvCopyState(indexName, indexId, this.id);
        primaryIndexVersion = copyState.getVersion();
        lastPrimaryGen = copyState.getPrimaryGen();
        break;
      } catch (Exception ignored) {
        try {
          Thread.sleep(NRT_CONNECT_WAIT_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (primaryIndexVersion == -1) {
      logger.info("Nrt sync: Could not get index version from primary, skipping");
      return;
    }
    long curVersion = getCurrentSearchingVersion();
    logger.info("Nrt sync: primary version: {}, my version: {}", primaryIndexVersion, curVersion);
    // Keep trying to sync a new nrt point until either we run out of time, our searcher version
    // updates, or we are unable to start a new copy job. This is needed since long running nrt
    // points may fail if the primary cleans up old commit files.
    while (curVersion < primaryIndexVersion && (System.currentTimeMillis() - startMS < maxTimeMs)) {
      CopyJob job = newNRTPoint(lastPrimaryGen, primaryIndexVersion);
      if (job == null) {
        // A job won't be started if we are already at the target version,
        // check if that is the case
        curVersion = getCurrentSearchingVersion();
        if (curVersion >= primaryIndexVersion) {
          break;
        }
        // The job failed to start for another reason, abort
        logger.info("Nrt sync: failed to start copy job, aborting");
        return;
      }
      logger.info(
          "Nrt sync: started new copy job, my version: {}, job version: {}",
          curVersion,
          job.getCopyState().version());
      while (true) {
        curVersion = getCurrentSearchingVersion();
        if (curVersion >= primaryIndexVersion) {
          break;
        }
        synchronized (this) {
          if (curNRTCopy == null) {
            curVersion = getCurrentSearchingVersion();
            break;
          }
        }
        try {
          Thread.sleep(NRT_SYNC_WAIT_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    logger.info("Finished syncing nrt point from current primary, current version: {}", curVersion);
  }

  @Override
  public FileMetaData readLocalFileMetaData(String fileName) throws IOException {
    return NrtUtils.readOnceLocalFileMetaData(fileName, lastFileMetaData, this);
  }
}
