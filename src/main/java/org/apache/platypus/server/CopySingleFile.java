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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Locale;

public class CopySingleFile extends CopyOneFile {
    private final Iterator<RawFileChunk> rawFileChunkIterator;
    private final IndexOutput out;
    private final ReplicaNode dest;
    private final long copyStartNS;
    private final byte[] buffer;
    private final String tempName;
    private long bytesCopied;
    public final String name;


    public final FileMetaData metaData;
    public final long bytesToCopy;
    private long remoteFileChecksum;


    public CopySingleFile(Iterator<RawFileChunk> rawFileChunkIterator, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer) throws IOException {
        super(null, dest, name, metaData, buffer);
        this.rawFileChunkIterator = rawFileChunkIterator;
        this.name = name;
        this.dest = dest;
        this.buffer = buffer;
        // TODO: ugly, we create a temp folder on top of a temp folder created by base class with tempName duplicated as well
        out = dest.createTempOutput(name, "copy", IOContext.DEFAULT);
        tempName = out.getName();
        // last 8 bytes are checksum:
        bytesToCopy = metaData.length - 8;
        if (Node.VERBOSE_FILES) {
            dest.message("file " + name + ": start copying to tmp file " + tmpName + " length=" + (8 + bytesToCopy));
        }
        copyStartNS = System.nanoTime();
        this.metaData = metaData;
    }

    public CopySingleFile(CopySingleFile other, Iterator<RawFileChunk> rawFileChunkIterator) {
        super(other, null);
        this.rawFileChunkIterator = rawFileChunkIterator;
        this.dest = other.dest;
        this.name = other.name;
        this.tempName = other.tmpName;
        this.out = other.out;
        this.metaData = other.metaData;
        this.bytesCopied = other.bytesCopied;
        this.bytesToCopy = other.bytesToCopy;
        this.copyStartNS = other.copyStartNS;
        this.buffer = other.buffer;
    }

    public String getTmpName() {
        return tempName;
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
            bytesCopied += byteString.size();
            if (bytesCopied < bytesToCopy) {
                out.writeBytes(byteString.toByteArray(), 0, byteString.size());
            } else { // last chunk, last 8 bytes are crc32 checksum
                out.writeBytes(byteString.toByteArray(), 0, byteString.size() - 8);
                remoteFileChecksum = ByteBuffer.wrap(byteString.substring(byteString.size() - 8, byteString.size()).toByteArray()).getLong();
            }
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
            long actualChecksumIn = remoteFileChecksum;
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
                        (System.nanoTime() - copyStartNS) / 1000000.0));
            }
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
        super.close();
        //ugh sad! delete temp file in base class, since "out" in base class is private and we have our own in this class.
        String tempFileResourceString = out.toString();
        Path unusedTempFile = Paths.get(tempFileResourceString.split("\"")[1].split("index")[0], "index", super.tmpName);
        Files.deleteIfExists(unusedTempFile);
    }

    @Override
    public long getBytesCopied() {
        return bytesCopied;
    }
}
