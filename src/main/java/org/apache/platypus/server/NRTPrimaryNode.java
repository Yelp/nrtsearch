package org.apache.platypus.server;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.*;
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
        int replicaId;
        ReplicationServerClient replicationServerClient;
        ReplicaDetails(int replicaId, ReplicationServerClient replicationServerClient) {
            replicaId = replicaId;
            replicationServerClient = replicationServerClient;
        }
    }

    /** Holds all replicas currently warming (pre-copying the new files) a single merged segment */
    static class MergePreCopy {
        final List<ReplicationServerClient> connections = Collections.synchronizedList(new ArrayList<>());
        final Map<String,FileMetaData> files;
        private boolean finished;

        public MergePreCopy(Map<String,FileMetaData> files) {
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
        System.out.println("NRTPrimaryNode: sendNRTPoint");
        // Something did get flushed (there were indexing ops since the last flush):

        // nocommit: we used to notify caller of the version, before trying to push to replicas, in case we crash after flushing but
        // before notifying all replicas, at which point we have a newer version index than client knew about?
        long version = getCopyStateVersion();

//        int[] replicaIDs;
//        InetSocketAddress[] replicaAddresses;
//        synchronized (this) {
//            replicaIDs = this.replicaIDs;
//            replicaAddresses = this.replicaAddresses;
//        }
//
//        message("send flushed version=" + version + " replica count " + replicaIDs.length);
//
//        // Notify current replicas:
//        for(int i=0;i<replicaIDs.length;i++) {
//            int replicaID = replicaIDs[i];
//            try (Connection c = new Connection(replicaAddresses[i])) {
//                message("send NEW_NRT_POINT to R" + replicaID + " at address=" + replicaAddresses[i]);
//                c.out.writeInt(Server.BINARY_MAGIC);
//                c.out.writeString("newNRTPoint");
//                c.out.writeString(indexName);
//                c.out.writeVLong(version);
//                c.out.writeVLong(primaryGen);
//                c.flush();
//                // TODO: we should use multicast to broadcast files out to replicas
//                // TODO: ... replicas could copy from one another instead of just primary
//                // TODO: we could also prioritize one replica at a time?
//            } catch (Throwable t) {
//                message("top: failed to connect R" + replicaID + " for newNRTPoint; skipping: " + t.getMessage());
//            }
//        }
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
        throw new UnsupportedEncodingException();
    }

    public void setRAMBufferSizeMB(double mb) {
        writer.getConfig().setRAMBufferSizeMB(mb);
    }

    public void addReplica(int replicaID, ReplicationServerClient replicationServerClient) throws IOException {

        message("add replica: " + warmingSegments.size() + " current warming merges");
        replicasInfos.add(new ReplicaDetails(replicaID, replicationServerClient));

        // Step through all currently warming segments and try to add this replica if it isn't there already:
        synchronized(warmingSegments) {
            for(MergePreCopy preCopy : warmingSegments) {
                logger.info(String.format("warming segment %s", preCopy.files.keySet()));
                message("warming segment " + preCopy.files.keySet());
                boolean found = false;
                synchronized (preCopy.connections) {
                    for(ReplicationServerClient each : preCopy.connections) {
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

                //TODO send out the copyFile request to the ReplicationServer
//                c.out.writeInt(Server.BINARY_MAGIC);
//                c.out.writeString("copyFiles");
//                c.out.writeString(indexName);
//                c.out.writeVLong(primaryGen);
//                CopyFilesHandler.writeFilesMetaData(c.out, preCopy.files);
//                c.flush();
//                c.s.shutdownOutput();
                logger.info("successfully started warming");
                message("successfully started warming");
            }
        }
    }

}
