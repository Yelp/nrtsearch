package org.apache.platypus.server;

import com.google.protobuf.ByteString;
import org.apache.lucene.replicator.nrt.CopyOneFile;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.replicator.nrt.ReplicaNode;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.platypus.server.grpc.RawFileChunk;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

public class CopySingleFile extends CopyOneFile {
    private final Iterator<RawFileChunk> rawFileChunkIterator;
    private final IndexOutput out;
    private final ReplicaNode dest;
    private final long copyStartNS;
    private final byte[] buffer;
    private long bytesCopied;
    public final String name;
    public final String tmpName;
    public final FileMetaData metaData;
    public final long bytesToCopy;


    public CopySingleFile(Iterator<RawFileChunk> rawFileChunkIterator, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer) throws IOException {
        super(null, dest, name, metaData, buffer);
        this.rawFileChunkIterator = rawFileChunkIterator;
        this.name = name;
        this.dest = dest;
        this.buffer = buffer;
        // TODO: pass correct IOCtx, e.g. seg total size
        out = dest.createTempOutput(name, "copy", IOContext.DEFAULT);
        tmpName = out.getName();
        // last 8 bytes are checksum:
        bytesToCopy = metaData.length - 8;
        if (Node.VERBOSE_FILES) {
            dest.message("file " + name + ": start copying to tmp file " + tmpName + " length=" + (8 + bytesToCopy));
        }
        copyStartNS = System.nanoTime();
        this.metaData = metaData;
        dest.startCopyFile(name);

    }

    public CopySingleFile(CopySingleFile other, Iterator<RawFileChunk> rawFileChunkIterator) {
        super(other, null);
        this.rawFileChunkIterator = rawFileChunkIterator;
        this.dest = other.dest;
        this.name = other.name;
        this.out = other.out;
        this.tmpName = other.tmpName;
        this.metaData = other.metaData;
        this.bytesCopied = other.bytesCopied;
        this.bytesToCopy = other.bytesToCopy;
        this.copyStartNS = other.copyStartNS;
        this.buffer = other.buffer;
    }

    /**
     * Copy another chunk of bytes, returning true once the copy is done
     */
    @Override
    public boolean visit() throws IOException {
        //TODO: add checksum validation? Primary sends entire file + checksum?
        if (rawFileChunkIterator.hasNext()) {
            RawFileChunk rawFileChunk = rawFileChunkIterator.next();
            ByteString byteString = rawFileChunk.getContent();
            out.writeBytes(byteString.toByteArray(), 0, byteString.size());
            bytesCopied += byteString.size();
            return false;
        } else {
            long checksum = out.getChecksum();
            if (checksum != metaData.checksum) {
                // Bits flipped during copy!
                dest.message("file " + tmpName + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum=" + checksum + " vs expected=" + metaData.checksum + "; cancel job");
                throw new IOException("file " + name + ": checksum mismatch after file copy");
            }
            // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an already corrupted file whose checksum (in its
            // footer) disagrees with reality:
            //TODO: Primary needs to send checksum for this to work at the end of file raw stream.
            // long actualChecksumIn = in.readLong();
            long actualChecksumIn = metaData.checksum;
            if (actualChecksumIn != checksum) {
                dest.message("file " + tmpName + ": checksum claimed by primary disagrees with the file's footer: claimed checksum=" + checksum + " vs actual=" + actualChecksumIn);
                throw new IOException("file " + name + ": checksum mismatch after file copy");
            }
            out.writeLong(checksum);
            close();
            if (Node.VERBOSE_FILES) {
                dest.message(String.format(Locale.ROOT, "file %s: done copying [%s, %.3fms]",
                        name,
                        Node.bytesToString(metaData.length),
                        (System.nanoTime() - copyStartNS)/1000000.0));
            }
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
        dest.finishCopyFile(name);
    }

    @Override
    public long getBytesCopied() {
        return bytesCopied;
    }
}
