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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * Abstract class to replace the Lucene CopyOneFile class, so that we can use separate
 * implementations for gRPC vs remote (S3) replication.
 */
public abstract class CopyOneFile implements Closeable {
  public final String name;
  public final String tmpName;
  public final FileMetaData metaData;
  public final long bytesToCopy;
  protected final IndexOutput out;
  protected final ReplicaNode dest;

  /**
   * Constructor
   *
   * @param dest destination ReplicaNode
   * @param name file name
   * @param metaData file metadata
   * @throws IOException on error
   */
  protected CopyOneFile(ReplicaNode dest, String name, FileMetaData metaData) throws IOException {
    this.name = name;
    this.dest = dest;
    // TODO: pass correct IOCtx, e.g. seg total size
    out = dest.createTempOutput(name, "copy", IOContext.DEFAULT);
    tmpName = out.getName();

    // last 8 bytes are checksum, which we write ourselves after copying all bytes and confirming
    // checksum:
    bytesToCopy = metaData.length() - Long.BYTES;

    if (dest.isVerboseFiles()) {
      dest.message(
          "file "
              + name
              + ": start copying to tmp file "
              + tmpName
              + " length="
              + (8 + bytesToCopy));
    }

    this.metaData = metaData;
  }

  /**
   * Get the number of bytes copied so far.
   *
   * @return number of bytes copied so far
   */
  public abstract long getBytesCopied();

  /**
   * Perform one unit of work copying the file.
   *
   * @return true if the file is fully copied, false otherwise
   * @throws IOException on error
   */
  public abstract boolean visit() throws IOException;

  @Override
  public void close() throws IOException {
    out.close();
    dest.finishCopyFile(name);
  }
}
