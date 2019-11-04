/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.platypus.server.luceneserver;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.platypus.server.grpc.FilesMetadata;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.apache.platypus.server.grpc.TransferStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NRTPrimaryNode extends PrimaryNode {
    private final InetSocketAddress localAddress;
    private final String indexName;
    Logger logger = LoggerFactory.getLogger(NRTPrimaryNode.class);
    final List<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());
    final Queue<ReplicaDetails> replicasInfos = new ConcurrentLinkedQueue();

    public NRTPrimaryNode(String indexName, InetSocketAddress localAddress, IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion,
                          SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
        super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
        this.localAddress = localAddress;
        this.indexName = indexName;
    }

    static class ReplicaDetails {
        private final int replicaId;

        public int getReplicaId() {
            return replicaId;
        }

        public ReplicationServerClient getReplicationServerClient() {
            return replicationServerClient;
        }

        private final ReplicationServerClient replicationServerClient;

        ReplicaDetails(int replicaId, ReplicationServerClient replicationServerClient) {
            this.replicaId = replicaId;
            this.replicationServerClient = replicationServerClient;
        }
    }

    /**
     * Holds all replicas currently warming (pre-copying the new files) a single merged segment
     */
    static class MergePreCopy {
        final List<ReplicationServerClient> connections = Collections.synchronizedList(new ArrayList<>());
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

        // nocommit: we used to notify caller of the version, before trying to push to replicas, in case we crash after flushing but
        // before notifying all replicas, at which point we have a newer version index than client knew about?
        long version = getCopyStateVersion();
        message("send flushed version=" + version + " replica count " + replicasInfos.size());

        // Notify current replicas:
        Iterator<ReplicaDetails> it = replicasInfos.iterator();
        while (it.hasNext()) {
            ReplicaDetails replicaDetails = it.next();
            int replicaID = replicaDetails.replicaId;
            ReplicationServerClient currentReplicaServerClient = replicaDetails.replicationServerClient;
            currentReplicaServerClient.newNRTPoint(indexName, primaryGen, version);
        }
    }

    // TODO: awkward we are forced to do this here ... this should really live in replicator code, e.g. PrimaryNode.mgr should be this:
    static class PrimaryNodeReferenceManager extends ReferenceManager<IndexSearcher> {
        final NRTPrimaryNode primary;
        final SearcherFactory searcherFactory;

        public PrimaryNodeReferenceManager(NRTPrimaryNode primary, SearcherFactory searcherFactory) throws IOException {
            this.primary = primary;
            this.searcherFactory = searcherFactory;
            current = SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), null);
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
                return SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), referenceToRefresh.getIndexReader());
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
    protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String, FileMetaData> files) throws IOException {
        if (replicasInfos.isEmpty()) {
            logger.info("no replicas, skip warming " + info);
            message("no replicas; skip warming " + info);
            return;
        }

        logger.info("top: warm merge " + info + " to " + replicasInfos.size() + " replicas; localAddress=" + localAddress + ": files=" + files.keySet());
        message("top: warm merge " + info + " to " + replicasInfos.size() + " replicas; localAddress=" + localAddress + ": files=" + files.keySet());

        MergePreCopy preCopy = new MergePreCopy(files);
        warmingSegments.add(preCopy);

        try {
            // Ask all currently known replicas to pre-copy this newly merged segment's files:
            Iterator<ReplicaDetails> replicaInfos = replicasInfos.iterator();
            ReplicaDetails replicaDetails = null;
            FilesMetadata filesMetadata = RecvCopyStateHandler.writeFilesMetaData(files);
            Map<ReplicationServerClient, Iterator<TransferStatus>> allCopyStatus = new ConcurrentHashMap<>();
            while (replicaInfos.hasNext()) {
                try {
                    replicaDetails = replicaInfos.next();
                    Iterator<TransferStatus> copyStatus = replicaDetails.replicationServerClient.copyFiles(indexName, primaryGen, filesMetadata);
                    allCopyStatus.put(replicaDetails.replicationServerClient, copyStatus);
                    message("warm connection " + replicaDetails.replicationServerClient.getHost() + ":" + replicaDetails.replicationServerClient.getPort());
                    preCopy.connections.add(replicaDetails.replicationServerClient);
                } catch (Throwable t) {
                    message("top: ignore exception trying to warm to replica for host:" + replicaDetails.replicationServerClient.getHost() + " port: " + replicaDetails.replicationServerClient.getPort() + ": " + t);
                    logger.info("top: ignore exception trying to warm to replica for host:" + replicaDetails.replicationServerClient.getHost() + " port: " + replicaDetails.replicationServerClient.getPort() + ": " + t);
                    //t.printStackTrace(System.out);
                }
            }

            long startNS = System.nanoTime();
            long lastWarnNS = startNS;

            // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow replica(s) to finish copying?
            while (preCopy.finished() == false) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    throw new ThreadInterruptedException(ie);
                }

                if (isClosed()) {
                    message("top: primary is closing: now cancel segment warming");
                    logger.info("top: primary is closing: now cancel segment warming");
                    //TODO: should we close these here?
//                    synchronized (preCopy.connections) {
//                        IOUtils.closeWhileHandlingException(preCopy.connections);
//                    }
                    return;
                }

                long ns = System.nanoTime();
                if (ns - lastWarnNS > 1000000000L) {
                    message(String.format(Locale.ROOT, "top: warning: still warming merge " + info + " to " + preCopy.connections.size() + " replicas for %.1f sec...", (ns - startNS) / 1000000000.0));
                    logger.info(String.format(Locale.ROOT, "top: warning: still warming merge " + info + " to " + preCopy.connections.size() + " replicas for %.1f sec...", (ns - startNS) / 1000000000.0));
                    lastWarnNS = ns;
                }

                // Because a replica can suddenly start up and "join" into this merge pre-copy:
                synchronized (preCopy.connections) {
                    Iterator<ReplicationServerClient> it = preCopy.connections.iterator();
                    while (it.hasNext()) {
                        ReplicationServerClient currentReplicationServerClient = it.next();
                        try {
                            Iterator<TransferStatus> transferStatusIterator = allCopyStatus.get(currentReplicationServerClient);
                            while (transferStatusIterator.hasNext()) {
                                TransferStatus transferStatus = transferStatusIterator.next();
                                logger.info("transferStatus for replicationServerClient=" + currentReplicationServerClient + " merge files=" + files.keySet() +
                                        " code: " + transferStatus.getCode() + " message: " + transferStatus.getMessage());
                            }
                            it.remove();
                        } catch (Throwable t) {
                            message("top: ignore exception trying to read byte during warm for segment=" + info + " to replica=" + currentReplicationServerClient + ": " + t + " files=" + files.keySet());
                            it.remove();
                        }
                    }
                }
            }
            message("top: done warming merge " + info);
        } finally {
            warmingSegments.remove(preCopy);
        }
    }


    public void setRAMBufferSizeMB(double mb) {
        writer.getConfig().setRAMBufferSizeMB(mb);
    }

    public void addReplica(int replicaID, ReplicationServerClient replicationServerClient) throws IOException {
        logger.info("add replica: " + warmingSegments.size() + " current warming merges ");
        message("add replica: " + warmingSegments.size() + " current warming merges ");

        replicasInfos.add(new ReplicaDetails(replicaID, replicationServerClient));

        // Step through all currently warming segments and try to add this replica if it isn't there already:
        synchronized (warmingSegments) {
            for (MergePreCopy preCopy : warmingSegments) {
                logger.info(String.format("warming segment %s", preCopy.files.keySet()));
                message("warming segment " + preCopy.files.keySet());
                boolean found = false;
                synchronized (preCopy.connections) {
                    for (ReplicationServerClient each : preCopy.connections) {
                        if (each.equals(replicationServerClient)) {
                            found = true;
                            break;
                        }
                    }
                }

                if (found) {
                    logger.info(String.format("this replica is already warming this segment; skipping"));
                    message("this replica is already warming this segment; skipping");
                    // It's possible (maybe) that the replica started up, then a merge kicked off, and it warmed to this new replica, all before the
                    // replica sent us this command:
                    continue;
                }

                // OK, this new replica is not already warming this segment, so attempt (could fail) to start warming now:
                if (preCopy.tryAddConnection(replicationServerClient) == false) {
                    // This can happen, if all other replicas just now finished warming this segment, and so we were just a bit too late.  In this
                    // case the segment must be copied over in the next nrt point sent to this replica
                    logger.info("failed to add connection to segment warmer (too late); closing");
                    message("failed to add connection to segment warmer (too late); closing");
                    //TODO: Close this and other replicationServerClient in close of this class? c.close();
                }

                FilesMetadata filesMetadata = RecvCopyStateHandler.writeFilesMetaData(preCopy.files);
                replicationServerClient.copyFiles(indexName, primaryGen, filesMetadata);
                logger.info("successfully started warming");
                message("successfully started warming");
            }
        }
    }

}
