package org.apache.platypus.server;

import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class NRTReplicaNode extends ReplicaNode {
    private final ReplicationServerClient primaryAddress;
    private final String indexName;
    final Jobs jobs;
    /* Just a wrapper class to hold our <hostName, port> pair so that we can send them to the Primary
     * on sendReplicas and it can build its channel over this pair */
    private final InetSocketAddress localSocketAddress;
    Logger logger = LoggerFactory.getLogger(NRTPrimaryNode.class);

    public NRTReplicaNode(String indexName, ReplicationServerClient primaryAddress, InetSocketAddress localSocketAddress,
                          int replicaId, Directory indexDir, SearcherFactory searcherFactory, PrintStream printStream,
                          long primaryGen) throws IOException {
        super(replicaId, indexDir, searcherFactory, printStream);
        this.primaryAddress = primaryAddress;
        this.indexName = indexName;
        this.localSocketAddress = localSocketAddress;
        // Handles fetching files from primary:
        jobs = new Jobs(this);
        jobs.setName("R" + id + ".copyJobs");
        jobs.setDaemon(true);
        jobs.start();
        start(primaryGen);
    }

    @Override
    protected CopyJob newCopyJob(String reason, Map<String, FileMetaData> files, Map<String, FileMetaData> prevFiles, boolean highPriority, CopyJob.OnceDone onceDone) throws IOException {
        CopyState copyState;

        //sendMeFiles(?) (we dont need this, just send Index,replica, and request for copy State)
        if(files == null) {
            // No incoming CopyState: ask primary for latest one now
            copyState = getCopyStateFromPrimary();
            files = copyState.files;
        }
        else {
            copyState = null;
        }
        return new SimpleCopyJob(reason, primaryAddress, copyState, this, files, highPriority, onceDone);
    }

    private CopyState getCopyStateFromPrimary() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void launch(CopyJob job) {
        jobs.launch(job);
    }

    /* called once start(primaryGen) is invoked on this object (see constructor) */
    @Override
    protected void sendNewReplica() throws IOException {
        logger.info(String.format("send new_replica to primary host=%s, tcpPort=%s",
                primaryAddress.getHost(), primaryAddress.getPort()));
        primaryAddress.addReplicas(indexName, this.id, localSocketAddress.getHostName(), 0);
    }

    public CopyJob launchPreCopyFiles(AtomicBoolean finished, long curPrimaryGen, Map<String,FileMetaData> files) throws IOException {
        return launchPreCopyMerge(finished, curPrimaryGen, files);
    }

    @Override
    public void close() throws IOException {
        //TODO: copy jobs running in their own thread
        //jobs.close();
        System.out.println("CLOSE NRT REPLICA");
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

}
