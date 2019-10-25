package org.apache.platypus.server;

import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.apache.platypus.server.grpc.FileMetadata;
import org.apache.platypus.server.grpc.FilesMetadata;
import org.apache.platypus.server.grpc.ReplicationServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
        // Handles fetching files from primary, on a new thread which receives files from primary
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
        if (files == null) {
            // No incoming CopyState: ask primary for latest one now
            copyState = getCopyStateFromPrimary();
            files = copyState.files;
        } else {
            copyState = null;
        }
        return new SimpleCopyJob(reason, primaryAddress, copyState, this, files, highPriority, onceDone);
    }

    private CopyState getCopyStateFromPrimary() throws IOException {
        org.apache.platypus.server.grpc.CopyState copyState = primaryAddress.recvCopyState(indexName, id);
        return readCopyState(copyState);
    }

    /**
     * Pulls CopyState off the wire
     */
    private static CopyState readCopyState(org.apache.platypus.server.grpc.CopyState copyState) throws IOException {

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

    public static Map<String, FileMetaData> readFilesMetaData(FilesMetadata filesMetadata) throws IOException {
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
        logger.info(String.format("send new_replica to primary host=%s, tcpPort=%s",
                primaryAddress.getHost(), primaryAddress.getPort()));
        primaryAddress.addReplicas(indexName, this.id, localSocketAddress.getHostName(), localSocketAddress.getPort());
    }

    public CopyJob launchPreCopyFiles(AtomicBoolean finished, long curPrimaryGen, Map<String, FileMetaData> files) throws IOException {
        return launchPreCopyMerge(finished, curPrimaryGen, files);
    }

    @Override
    public void close() throws IOException {
        //TODO: copy jobs running in their own thread
        //jobs.close();
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

}
