package org.apache.platypus.server;

import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class NRTReplicaNode extends ReplicaNode {
    public NRTReplicaNode(int id, Directory dir, SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
        super(id, dir, searcherFactory, printStream);
    }

    @Override
    protected CopyJob newCopyJob(String reason, Map<String, FileMetaData> files, Map<String, FileMetaData> prevFiles, boolean highPriority, CopyJob.OnceDone onceDone) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void launch(CopyJob job) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void sendNewReplica() throws IOException {
        throw new UnsupportedOperationException();
    }
}
