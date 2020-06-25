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
package org.apache.lucene.replicator.nrt;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Locale;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

public class CopyOneFile implements Closeable {
  private final Iterator<RawFileChunk> rawFileChunkIterator;
  private final IndexOutput out;
  private final ReplicaNode dest;
  public final String name;
  public final String tmpName;
  public final FileMetaData metaData;
  public final long bytesToCopy;
  private final long copyStartNS;
  private final byte[] buffer;

  private long bytesCopied;
  private long remoteFileChecksum;

  public CopyOneFile(
      Iterator<RawFileChunk> rawFileChunkIterator,
      ReplicaNode dest,
      String name,
      FileMetaData metaData,
      byte[] buffer)
      throws IOException {

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
      dest.message(
          "file "
              + name
              + ": start copying to tmp file "
              + tmpName
              + " length="
              + (8 + bytesToCopy));
    }
    copyStartNS = System.nanoTime();
    this.metaData = metaData;
    dest.startCopyFile(name);
  }

  /** Transfers this file copy to another input, continuing where the first one left off */
  public CopyOneFile(CopyOneFile other, Iterator<RawFileChunk> rawFileChunkIterator) {
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
   * Closes this stream and releases any system resources associated with it. If the stream is
   * already closed then invoking this method has no effect.
   *
   * <p>As noted in {@link AutoCloseable#close()}, cases where the close may fail require careful
   * attention. It is strongly advised to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    out.close();
    dest.finishCopyFile(name);
  }

  public long getBytesCopied() {
    return bytesCopied;
  }

  /** Copy another chunk of bytes, returning true once the copy is done */
  public boolean visit() throws IOException {
    if (rawFileChunkIterator.hasNext()) {
      RawFileChunk rawFileChunk = rawFileChunkIterator.next();
      ByteString byteString = rawFileChunk.getContent();
      bytesCopied += byteString.size();
      if (bytesCopied < bytesToCopy) {
        out.writeBytes(byteString.toByteArray(), 0, byteString.size());
      } else { // last chunk, last 8 bytes are crc32 checksum
        out.writeBytes(byteString.toByteArray(), 0, byteString.size() - 8);
        remoteFileChecksum =
            ByteBuffer.wrap(
                    byteString.substring(byteString.size() - 8, byteString.size()).toByteArray())
                .getLong();
        bytesCopied -= 8;
      }
      return false;
    } else {
      long checksum = out.getChecksum();
      if (checksum != metaData.checksum) {
        // Bits flipped during copy!
        dest.message(
            "file "
                + tmpName
                + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum="
                + checksum
                + " vs expected="
                + metaData.checksum
                + "; cancel job");
        throw new IOException("file " + name + ": checksum mismatch after file copy");
      }
      // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an already
      // corrupted file whose checksum (in its
      // footer) disagrees with reality:
      long actualChecksumIn = remoteFileChecksum;
      if (actualChecksumIn != checksum) {
        dest.message(
            "file "
                + tmpName
                + ": checksum claimed by primary disagrees with the file's footer: claimed checksum="
                + checksum
                + " vs actual="
                + actualChecksumIn);
        throw new IOException("file " + name + ": checksum mismatch after file copy");
      }
      out.writeLong(checksum);
      close();
      if (Node.VERBOSE_FILES) {
        dest.message(
            String.format(
                Locale.ROOT,
                "file %s: done copying [%s, %.3fms]",
                name,
                Node.bytesToString(metaData.length),
                (System.nanoTime() - copyStartNS) / 1000000.0));
      }
      return true;
    }
  }
}
