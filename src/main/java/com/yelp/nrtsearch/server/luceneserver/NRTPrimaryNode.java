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

import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.ThreadInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NRTPrimaryNode extends PrimaryNode {
  private static final Logger logger = LoggerFactory.getLogger(NRTPrimaryNode.class);
  private final HostPort hostPort;
  private final String indexName;
  final List<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());
  final Queue<ReplicaDetails> replicasInfos = new ConcurrentLinkedQueue<>();

  public NRTPrimaryNode(
      String indexName,
      HostPort hostPort,
      IndexWriter writer,
      int id,
      long primaryGen,
      long forcePrimaryVersion,
      SearcherFactory searcherFactory,
      PrintStream printStream)
      throws IOException {
    super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
    this.hostPort = hostPort;
    this.indexName = indexName;
  }

  public static class ReplicaDetails {
    private final int replicaId;
    private final HostPort hostPort;
    private final ReplicationServerClient replicationServerClient;

    public int getReplicaId() {
      return replicaId;
    }

    public ReplicationServerClient getReplicationServerClient() {
      return replicationServerClient;
    }

    public HostPort getHostPort() {
      return hostPort;
    }

    ReplicaDetails(int replicaId, ReplicationServerClient replicationServerClient) {
      this.replicaId = replicaId;
      this.replicationServerClient = replicationServerClient;
      this.hostPort =
          new HostPort(replicationServerClient.getHost(), replicationServerClient.getPort());
    }

    /*
     * WARNING: Do not replace this with the IDE autogenerated equals method. We put this object in a ConcurrentQueue
     * and this is the check that we need for equality.
     * */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReplicaDetails that = (ReplicaDetails) o;
      return replicaId == that.replicaId && Objects.equals(hostPort, that.hostPort);
    }

    /*
     * WARNING: Do not replace this with the IDE autogenerated hashCode method. We put this object in a ConcurrentQueue
     * and this is the check that we need for hashCode.
     * */
    @Override
    public int hashCode() {
      return Objects.hash(replicaId, hostPort);
    }
  }

  /** Holds all replicas currently warming (pre-copying the new files) a single merged segment */
  static class MergePreCopy {
    final Set<ReplicationServerClient> connections = Collections.synchronizedSet(new HashSet<>());
    final Map<String, FileMetaData> files;
    private boolean finished;

    public MergePreCopy(Map<String, FileMetaData> files) {
      this.files = files;
    }

    public synchronized boolean tryAddConnection(ReplicationServerClient c) {
      if (finished == false) {
        connections.add(c);
        return true;
      } else {
        return false;
      }
    }

    public synchronized boolean finished() {
      if (connections.isEmpty()) {
        finished = true;
        return true;
      } else {
        return false;
      }
    }
  }

  void sendNewNRTPointToReplicas() {
    logger.info("NRTPrimaryNode: sendNRTPoint");
    // Something did get flushed (there were indexing ops since the last flush):

    // nocommit: we used to notify caller of the version, before trying to push to replicas, in case
    // we crash after flushing but
    // before notifying all replicas, at which point we have a newer version index than client knew
    // about?
    long version = getCopyStateVersion();
    logMessage("send flushed version=" + version + " replica count " + replicasInfos.size());
    NrtMetrics.searcherVersion.labels(indexName).set(version);
    NrtMetrics.nrtPrimaryPointCount.labels(indexName).inc();

    // Notify current replicas:
    Iterator<ReplicaDetails> it = replicasInfos.iterator();
    while (it.hasNext()) {
      ReplicaDetails replicaDetails = it.next();
      int replicaID = replicaDetails.replicaId;
      ReplicationServerClient currentReplicaServerClient = replicaDetails.replicationServerClient;
      try {
        currentReplicaServerClient.newNRTPoint(indexName, primaryGen, version);
      } catch (StatusRuntimeException e) {
        Status status = e.getStatus();
        if (status.getCode().equals(Status.UNAVAILABLE.getCode())) {
          logger.warn(
              "NRTPRimaryNode: sendNRTPoint, lost connection to replicaId: {} host: {} port: {}",
              replicaDetails.replicaId,
              replicaDetails.replicationServerClient.getHost(),
              replicaDetails.replicationServerClient.getPort());
          currentReplicaServerClient.close();
          it.remove();
        }
      } catch (Exception e) {
        String msg =
            String.format(
                "top: failed to connect R%d for newNRTPoint; skipping: %s",
                replicaID, e.getMessage());
        message(msg);
        logger.warn(msg);
      }
    }
  }

  // TODO: awkward we are forced to do this here ... this should really live in replicator code,
  // e.g. PrimaryNode.mgr should be this:
  static class PrimaryNodeReferenceManager extends ReferenceManager<IndexSearcher> {
    final NRTPrimaryNode primary;
    final SearcherFactory searcherFactory;

    public PrimaryNodeReferenceManager(NRTPrimaryNode primary, SearcherFactory searcherFactory)
        throws IOException {
      this.primary = primary;
      this.searcherFactory = searcherFactory;
      current =
          SearcherManager.getSearcher(
              searcherFactory, primary.mgr.acquire().getIndexReader(), null);
    }

    @Override
    protected void decRef(IndexSearcher reference) throws IOException {
      reference.getIndexReader().decRef();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
      if (primary.flushAndRefresh()) {
        primary.sendNewNRTPointToReplicas();
        // NOTE: steals a ref from one ReferenceManager to another!
        return SearcherManager.getSearcher(
            searcherFactory,
            primary.mgr.acquire().getIndexReader(),
            referenceToRefresh.getIndexReader());
      } else {
        return null;
      }
    }

    @Override
    protected boolean tryIncRef(IndexSearcher reference) {
      return reference.getIndexReader().tryIncRef();
    }

    @Override
    protected int getRefCount(IndexSearcher reference) {
      return reference.getIndexReader().getRefCount();
    }
  }

  @Override
  protected void preCopyMergedSegmentFiles(
      SegmentCommitInfo info, Map<String, FileMetaData> files) {
    long mergeStartNS = System.nanoTime();
    if (replicasInfos.isEmpty()) {
      logMessage("no replicas, skip warming " + info);
      return;
    }

    MergePreCopy preCopy = new MergePreCopy(files);
    synchronized (warmingSegments) {
      logMessage(
          String.format(
              "top: warm merge %s to %d replicas; localAddress=%s: files=%s",
              info, replicasInfos.size(), hostPort, files.keySet()));

      warmingSegments.add(preCopy);
    }

    try {
      // Ask all currently known replicas to pre-copy this newly merged segment's files:
      Iterator<ReplicaDetails> replicaInfos = replicasInfos.iterator();
      ReplicaDetails replicaDetails = null;
      FilesMetadata filesMetadata = RecvCopyStateHandler.writeFilesMetaData(files);
      Map<ReplicationServerClient, Iterator<TransferStatus>> allCopyStatus =
          new ConcurrentHashMap<>();
      while (replicaInfos.hasNext()) {
        try {
          replicaDetails = replicaInfos.next();
          Iterator<TransferStatus> copyStatus =
              replicaDetails.replicationServerClient.copyFiles(
                  indexName, primaryGen, filesMetadata);
          allCopyStatus.put(replicaDetails.replicationServerClient, copyStatus);
          logMessage(
              String.format(
                  "warm connection %s:%d",
                  replicaDetails.replicationServerClient.getHost(),
                  replicaDetails.replicationServerClient.getPort()));
          preCopy.connections.add(replicaDetails.replicationServerClient);
        } catch (Throwable t) {
          logMessage(
              String.format(
                  "top: ignore exception trying to warm to replica for host:%s port: %d: %s",
                  replicaDetails.replicationServerClient.getHost(),
                  replicaDetails.replicationServerClient.getPort(),
                  t));
        }
      }

      long startNS = System.nanoTime();
      long lastWarnNS = startNS;

      // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow
      // replica(s) to finish copying?
      while (preCopy.finished() == false) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }

        if (isClosed()) {
          logMessage("top: primary is closing: now cancel segment warming");
          // TODO: should we close these here?
          //                    synchronized (preCopy.connections) {
          //                        IOUtils.closeWhileHandlingException(preCopy.connections);
          //                    }
          return;
        }

        long ns = System.nanoTime();
        if (ns - lastWarnNS > 1000000000L) {
          logMessage(
              String.format(
                  Locale.ROOT,
                  "top: warning: still warming merge %s to %d replicas for %.1f sec...",
                  info,
                  preCopy.connections.size(),
                  (ns - startNS) / 1000000000.0));
          lastWarnNS = ns;
        }

        // Because a replica can suddenly start up and "join" into this merge pre-copy:
        List<ReplicationServerClient> currentConnections = new ArrayList<>(preCopy.connections);
        for (ReplicationServerClient currentReplicationServerClient : currentConnections) {
          try {
            Iterator<TransferStatus> transferStatusIterator =
                allCopyStatus.get(currentReplicationServerClient);
            while (transferStatusIterator.hasNext()) {
              TransferStatus transferStatus = transferStatusIterator.next();
              logger.debug(
                  "transferStatus for replicationServerClient={},  merge files={}, code={}, message={}",
                  currentReplicationServerClient,
                  files.keySet(),
                  transferStatus.getCode(),
                  transferStatus.getMessage());
            }
          } catch (Throwable t) {
            String msg =
                String.format(
                    "top: ignore exception trying to read byte during warm for segment=%s to replica=%s: %s files=%s",
                    info, currentReplicationServerClient, t, files.keySet());
            logger.warn(msg, t);
            super.message(msg);
          }
        }
        currentConnections.forEach(preCopy.connections::remove);
      }
      logMessage("top: done warming merge " + info);
    } finally {
      warmingSegments.remove(preCopy);

      // record metrics for this merge
      NrtMetrics.nrtPrimaryMergeTime
          .labels(indexName)
          .observe((System.nanoTime() - mergeStartNS) / 1000000.0);
    }
  }

  public void logMessage(String msg) {
    message(msg);
    logger.info(msg);
  }

  public void setRAMBufferSizeMB(double mb) {
    writer.getConfig().setRAMBufferSizeMB(mb);
  }

  public void addReplica(int replicaID, ReplicationServerClient replicationServerClient)
      throws IOException {
    logMessage("add replica: " + warmingSegments.size() + " current warming merges ");
    ReplicaDetails replicaDetails = new ReplicaDetails(replicaID, replicationServerClient);
    if (!replicasInfos.contains(replicaDetails)) {
      replicasInfos.add(replicaDetails);
    }
    // Step through all currently warming segments and try to add this replica if it isn't there
    // already:
    synchronized (warmingSegments) {
      for (MergePreCopy preCopy : warmingSegments) {
        logger.debug("warming segment {}", preCopy.files.keySet());
        message("warming segment " + preCopy.files.keySet());
        synchronized (preCopy.connections) {
          if (preCopy.connections.contains(replicationServerClient)) {
            logMessage("this replica is already warming this segment; skipping");
            // It's possible (maybe) that the replica started up, then a merge kicked off, and it
            // warmed to this new replica, all before the
            // replica sent us this command:
            continue;
          }
        }

        // OK, this new replica is not already warming this segment, so attempt (could fail) to
        // start warming now:
        if (preCopy.tryAddConnection(replicationServerClient) == false) {
          // This can happen, if all other replicas just now finished warming this segment, and so
          // we were just a bit too late.  In this
          // case the segment must be copied over in the next nrt point sent to this replica
          logMessage("failed to add connection to segment warmer (too late); closing");
          // TODO: Close this and other replicationServerClient in close of this class? c.close();
        }

        FilesMetadata filesMetadata = RecvCopyStateHandler.writeFilesMetaData(preCopy.files);
        replicationServerClient.copyFiles(indexName, primaryGen, filesMetadata);
        logMessage("successfully started warming");
      }
    }
  }

  public Collection<ReplicaDetails> getNodesInfo() {
    return Collections.unmodifiableCollection(replicasInfos);
  }

  @Override
  public void close() throws IOException {
    logger.info("CLOSE NRT PRIMARY");
    Iterator<ReplicaDetails> it = replicasInfos.iterator();
    while (it.hasNext()) {
      ReplicaDetails replicaDetails = it.next();
      ReplicationServerClient replicationServerClient = replicaDetails.getReplicationServerClient();
      HostPort replicaHostPort = replicaDetails.getHostPort();
      logger.info(
          "CLOSE NRT PRIMARY, closing replica channel host:{}, port:{}",
          replicaHostPort.getHostName(),
          replicaHostPort.getPort());
      replicationServerClient.close();
      it.remove();
    }
    super.close();
  }
}
