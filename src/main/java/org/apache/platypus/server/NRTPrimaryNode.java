package org.apache.platypus.server;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.SearcherFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class NRTPrimaryNode extends PrimaryNode {
    public NRTPrimaryNode(IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion, SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
        super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
    }

    @Override
    protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String, FileMetaData> files) throws IOException {
        throw new UnsupportedEncodingException();
    }

    public void setRAMBufferSizeMB(double mb) {
        writer.getConfig().setRAMBufferSizeMB(mb);
    }

}
