/*
 * Copyright 2025 Yelp Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/** Copies one file from an incoming DataInput to a dest filename in a local Directory */
public class StreamCopyOneFile implements CopyOneFile {
  private final DataInput in;
  private final IndexOutput out;
  private final ReplicaNode dest;
  public final String name;
  public final String tmpName;
  public final FileMetaData metaData;
  public final long bytesToCopy;
  private final long copyStartNS;
  private final byte[] buffer;

  private long bytesCopied;

  public StreamCopyOneFile(
      DataInput in, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer)
      throws IOException {
    this.in = in;
    this.name = name;
    this.dest = dest;
    this.buffer = buffer;
    // TODO: pass correct IOCtx, e.g. seg total size
    out = dest.createTempOutput(name, "copy", IOContext.DEFAULT);
    tmpName = out.getName();

    // last 8 bytes are checksum, which we write ourselves after copying all bytes and confirming
    // checksum:
    bytesToCopy = metaData.length() - Long.BYTES;

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

  @Override
  public void close() throws IOException {
    if (in instanceof Closeable closeable) {
      closeable.close();
    }
    out.close();
    dest.finishCopyFile(name);
  }

  /** Copy another chunk of bytes, returning true once the copy is done */
  public boolean visit() throws IOException {
    long bytesLeft = bytesToCopy - bytesCopied;
    if (bytesLeft == 0) {
      long checksum = out.getChecksum();
      if (checksum != metaData.checksum()) {
        // Bits flipped during copy!
        dest.message(
            "file "
                + tmpName
                + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum="
                + checksum
                + " vs expected="
                + metaData.checksum()
                + "; cancel job");
        throw new IOException("file " + name + ": checksum mismatch after file copy");
      }

      // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an
      // already corrupted file whose checksum (in its
      // footer) disagrees with reality:
      long actualChecksumIn = CodecUtil.readBELong(in);
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
      CodecUtil.writeBELong(out, checksum);
      close();

      if (Node.VERBOSE_FILES) {
        dest.message(
            String.format(
                Locale.ROOT,
                "file %s: done copying [%s, %.3fms]",
                name,
                Node.bytesToString(metaData.length()),
                (System.nanoTime() - copyStartNS) / (double) TimeUnit.MILLISECONDS.toNanos(1)));
      }

      return true;
    }

    int toCopy = (int) Math.min(bytesLeft, buffer.length);
    in.readBytes(buffer, 0, toCopy);
    out.writeBytes(buffer, 0, toCopy);

    // TODO: rsync will fsync a range of the file; maybe we should do that here for large files in
    // case we crash/killed
    bytesCopied += toCopy;

    return false;
  }

  @Override
  public FileMetaData getFileMetaData() {
    return metaData;
  }

  @Override
  public String getFileName() {
    return name;
  }

  @Override
  public String getFileTmpName() {
    return tmpName;
  }

  @Override
  public long getBytesToCopy() {
    return bytesToCopy;
  }

  public long getBytesCopied() {
    return bytesCopied;
  }
}
