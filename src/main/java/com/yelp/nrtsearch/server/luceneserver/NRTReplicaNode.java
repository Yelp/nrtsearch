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

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.FileMetadata;
import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.grpc.GetNodesResponse;
import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.FilteringSegmentInfosSearcherManager;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;
import org.apache.lucene.replicator.nrt.ReplicaDeleterManager;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRTReplicaNode extends ReplicaNode {
  private static final long NRT_SYNC_WAIT_MS = 2000;
  private static final long NRT_CONNECT_WAIT_MS = 500;

  private final ReplicationServerClient primaryAddress;
  private final ReplicaDeleterManager replicaDeleterManager;
  private final String indexName;
  private final boolean ackedCopy;
  private final boolean filterIncompatibleSegmentReaders;
  final Jobs jobs;

  /* Just a wrapper class to hold our <hostName, port> pair so that we can send them to the Primary
   * on sendReplicas and it can build its channel over this pair */
  private final HostPort hostPort;
  private static final Logger logger = LoggerFactory.getLogger(NRTReplicaNode.class);

  public NRTReplicaNode(
      String indexName,
      ReplicationServerClient primaryAddress,
      HostPort hostPort,
      int replicaId,
      Directory indexDir,
      SearcherFactory searcherFactory,
      PrintStream printStream,
      boolean ackedCopy,
      boolean decInitialCommit,
      boolean filterIncompatibleSegmentReaders)
      throws IOException {
    super(replicaId, indexDir, searcherFactory, printStream);
    this.primaryAddress = primaryAddress;
    this.indexName = indexName;
    this.ackedCopy = ackedCopy;
    this.hostPort = hostPort;
    replicaDeleterManager = decInitialCommit ? new ReplicaDeleterManager(this) : null;
    this.filterIncompatibleSegmentReaders = filterIncompatibleSegmentReaders;
    // Handles fetching files from primary, on a new thread which receives files from primary
    jobs = new Jobs(this);
    jobs.setName("R" + id + ".copyJobs");
    jobs.setDaemon(true);
    jobs.start();
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
      mgr = new FilteringSegmentInfosSearcherManager(getDirectory(), this, mgr, searcherFactory);
    }
  }

  @Override
  protected CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    CopyState copyState;

    // sendMeFiles(?) (we dont need this, just send Index,replica, and request for copy State)
    if (files == null) {
      // No incoming CopyState: ask primary for latest one now
      try {
        // Exceptions in here mean something went wrong talking over the socket, which are fine
        // (e.g. primary node crashed):
        copyState = getCopyStateFromPrimary();
      } catch (Throwable t) {
        throw new NodeCommunicationException("exc while reading files to copy", t);
      }
      files = copyState.files;
    } else {
      copyState = null;
    }
    return new SimpleCopyJob(
        reason,
        primaryAddress,
        copyState,
        this,
        files,
        highPriority,
        onceDone,
        indexName,
        ackedCopy);
  }

  private CopyState getCopyStateFromPrimary() throws IOException {
    com.yelp.nrtsearch.server.grpc.CopyState copyState =
        primaryAddress.recvCopyState(indexName, id);
    return readCopyState(copyState);
  }

  /** Pulls CopyState off the wire */
  private static CopyState readCopyState(com.yelp.nrtsearch.server.grpc.CopyState copyState)
      throws IOException {

    // Decode a new CopyState
    byte[] infosBytes = new byte[copyState.getInfoBytesLength()];
    copyState.getInfoBytes().copyTo(ByteBuffer.wrap(infosBytes));

    long gen = copyState.getGen();
    long version = copyState.getVersion();
    Map<String, FileMetaData> files = readFilesMetaData(copyState.getFilesMetadata());

    int count = copyState.getCompletedMergeFilesCount();
    assert count == copyState.getCompletedMergeFilesCount();

    Set<String> completedMergeFiles = new HashSet<>();
    for (String completedMergeFile : copyState.getCompletedMergeFilesList()) {
      completedMergeFiles.add(completedMergeFile);
    }
    long primaryGen = copyState.getPrimaryGen();

    return new CopyState(files, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
  }

  public static Map<String, FileMetaData> readFilesMetaData(FilesMetadata filesMetadata)
      throws IOException {
    int fileCount = filesMetadata.getNumFiles();
    assert fileCount == filesMetadata.getFileMetadataCount();

    Map<String, FileMetaData> files = new HashMap<>();
    for (FileMetadata fileMetadata : filesMetadata.getFileMetadataList()) {
      String fileName = fileMetadata.getFileName();
      long length = fileMetadata.getLen();
      long checksum = fileMetadata.getChecksum();
      byte[] header = new byte[fileMetadata.getHeaderLength()];
      fileMetadata.getHeader().copyTo(ByteBuffer.wrap(header));
      byte[] footer = new byte[fileMetadata.getFooterLength()];
      fileMetadata.getFooter().copyTo(ByteBuffer.wrap(footer));
      files.put(fileName, new FileMetaData(header, footer, length, checksum));
    }
    return files;
  }

  @Override
  protected void launch(CopyJob job) {
    jobs.launch(job);
  }

  /* called once start(primaryGen) is invoked on this object (see constructor) */
  @Override
  protected void sendNewReplica() throws IOException {
    logger.info(String.format("send new_replica to primary: %s", primaryAddress));
    primaryAddress.addReplicas(indexName, this.id, hostPort.getHostName(), hostPort.getPort());
  }

  public CopyJob launchPreCopyFiles(
      AtomicBoolean finished, long curPrimaryGen, Map<String, FileMetaData> files)
      throws IOException {
    return launchPreCopyMerge(finished, curPrimaryGen, files);
  }

  @Override
  protected void finishNRTCopy(CopyJob job, long startNS) throws IOException {
    super.finishNRTCopy(job, startNS);

    // record metrics for this nrt point
    if (job.getFailed()) {
      NrtMetrics.nrtPointFailure.labels(indexName).inc();
    } else {
      NrtMetrics.nrtPointTime.labels(indexName).observe((System.nanoTime() - startNS) / 1000000.0);
      NrtMetrics.nrtPointSize.labels(indexName).observe(job.getTotalBytesCopied());
      NrtMetrics.searcherVersion.labels(indexName).set(job.getCopyState().version);
    }
  }

  @Override
  public void close() throws IOException {
    jobs.close();
    logger.info("CLOSE NRT REPLICA");
    message("top: jobs closed");
    synchronized (mergeCopyJobs) {
      for (CopyJob job : mergeCopyJobs) {
        message("top: cancel merge copy job " + job);
        job.cancel("jobs closing", null);
      }
    }
    primaryAddress.close();
    super.close();
  }

  @VisibleForTesting
  public ReplicaDeleterManager getReplicaDeleterManager() {
    return replicaDeleterManager;
  }

  public ReplicationServerClient getPrimaryAddress() {
    return primaryAddress;
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  /* returns true if present in primary's current list of known replicas else false.
  Throws StatusRuntimeException if cannot reach Primary */
  public boolean isKnownToPrimary() {
    GetNodesResponse getNodesResponse = primaryAddress.getConnectedNodes(indexName);
    for (NodeInfo nodeInfo : getNodesResponse.getNodesList()) {
      if (hostPort.equals(new HostPort(nodeInfo.getHostname(), nodeInfo.getPort()))) {
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
   * @param primaryWaitMs how long to wait for primary to be available
   * @param maxTimeMs max time to attempt initial point sync
   * @throws IOException on issue getting searcher version
   */
  public void syncFromCurrentPrimary(long primaryWaitMs, long maxTimeMs) throws IOException {
    logger.info("Starting sync of next nrt point from current primary");
    long startMS = System.currentTimeMillis();
    long primaryIndexVersion = -1;
    long lastPrimaryGen = -1;
    // Attempt to get the current index version from the primary, give up if the timeout
    // is exceeded
    while (System.currentTimeMillis() - startMS < primaryWaitMs) {
      try {
        com.yelp.nrtsearch.server.grpc.CopyState copyState =
            primaryAddress.recvCopyState(indexName, this.id);
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
      CopyJob job = newNRTPoint(lastPrimaryGen, Long.MAX_VALUE);
      if (job == null) {
        logger.info("Nrt sync: failed to start copy job, aborting");
        return;
      }
      logger.info("Nrt sync: started new copy job");
      while (true) {
        curVersion = getCurrentSearchingVersion();
        if (curVersion >= primaryIndexVersion) {
          break;
        }
        synchronized (this) {
          if (curNRTCopy == null) {
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
}
