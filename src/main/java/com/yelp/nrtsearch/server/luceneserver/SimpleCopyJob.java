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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyOneFile;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;
import org.apache.lucene.replicator.nrt.ReplicaNode;

public class SimpleCopyJob extends CopyJob {
  final byte[] copyBuffer = new byte[65536];
  private final CopyState copyState;
  private final ReplicationServerClient primaryAddres;
  private final String indexName;
  private Iterator<Map.Entry<String, FileMetaData>> iter;

  public SimpleCopyJob(
      String reason,
      ReplicationServerClient primaryAddress,
      CopyState copyState,
      ReplicaNode dest,
      Map<String, FileMetaData> files,
      boolean highPriority,
      OnceDone onceDone,
      String indexName)
      throws IOException {
    super(reason, files, dest, highPriority, onceDone);
    this.copyState = copyState;
    this.primaryAddres = primaryAddress;
    this.indexName = indexName;
  }

  @Override
  protected CopyOneFile newCopyOneFile(CopyOneFile prev) {
    Iterator<RawFileChunk> rawFileChunkIterator;
    try {
      rawFileChunkIterator = primaryAddres.recvRawFile(prev.name, prev.getBytesCopied(), indexName);
    } catch (Throwable t) {
      try {
        cancel("exc during start", t);
      } catch (IOException e) {
        throw new NodeCommunicationException("cancel IOException during newCopyOneFile", e);
      }
      throw new NodeCommunicationException("exc during start", t);
    }

    return new CopyOneFile(prev, rawFileChunkIterator);
  }

  @Override
  public void start() throws IOException {
    if (iter == null) {
      iter = toCopy.iterator();
      // This means we resumed an already in-progress copy; we do this one first:
      if (current != null) {
        totBytes += current.metaData.length;
      }
      for (Map.Entry<String, FileMetaData> ent : toCopy) {
        FileMetaData metaData = ent.getValue();
        totBytes += metaData.length;
      }

      // Send all file names / offsets up front to avoid ping-ping latency:
      try {
        dest.message(
            "SimpleCopyJob.init: done start files count="
                + toCopy.size()
                + " totBytes="
                + totBytes);
      } catch (Throwable t) {
        cancel("exc during start", t);
        throw new NodeCommunicationException("exc during start", t);
      }
    } else {
      throw new IllegalStateException("already started");
    }
  }

  @Override
  public void runBlocking() throws Exception {
    while (visit() == false) ;
    if (getFailed()) {
      throw new RuntimeException("copy failed: " + cancelReason, exc);
    }
  }

  @Override
  public boolean conflicts(CopyJob _other) {
    Set<String> filesToCopy = new HashSet<>();
    for (Map.Entry<String, FileMetaData> ent : toCopy) {
      filesToCopy.add(ent.getKey());
    }

    SimpleCopyJob other = (SimpleCopyJob) _other;
    synchronized (other) {
      for (Map.Entry<String, FileMetaData> ent : other.toCopy) {
        if (filesToCopy.contains(ent.getKey())) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public void finish() throws IOException {
    dest.message(
        String.format(
            Locale.ROOT,
            "top: file copy done; took %.1f msec to copy %d bytes (%.2f MB/sec); now rename %d tmp files",
            (System.nanoTime() - startNS) / 1000000.0,
            totBytesCopied,
            (totBytesCopied / 1024. / 1024.) / ((System.nanoTime() - startNS) / 1000000000.0),
            copiedFiles.size()));

    // NOTE: if any of the files we copied overwrote a file in the current commit point, we
    // (ReplicaNode) removed the commit point up
    // front so that the commit is not corrupt.  This way if we hit exc here, or if we crash here,
    // we won't leave a corrupt commit in
    // the index:
    for (Map.Entry<String, String> ent : copiedFiles.entrySet()) {
      String tmpFileName = ent.getValue();
      String fileName = ent.getKey();

      if (Node.VERBOSE_FILES) {
        dest.message("rename file " + tmpFileName + " to " + fileName);
      }

      // NOTE: if this throws exception, then some files have been moved to their true names, and
      // others are leftover .tmp files.  I don't
      // think heroic exception handling is necessary (no harm will come, except some leftover
      // files),  nor warranted here (would make the
      // code more complex, for the exceptional cases when something is wrong w/ your IO system):
      dest.getDirectory().rename(tmpFileName, fileName);
    }

    // nocommit syncMetaData here?
    copiedFiles.clear();
  }

  @Override
  public boolean getFailed() {
    return exc != null;
  }

  @Override
  public Set<String> getFileNamesToCopy() {
    Set<String> fileNames = new HashSet<>();
    for (Map.Entry<String, FileMetaData> ent : toCopy) {
      fileNames.add(ent.getKey());
    }
    return fileNames;
  }

  @Override
  public Set<String> getFileNames() {
    return files.keySet();
  }

  @Override
  public CopyState getCopyState() {
    return copyState;
  }

  @Override
  public long getTotalBytesCopied() {
    return totBytesCopied;
  }

  /** Higher priority and then "first come first serve" order. */
  @Override
  public int compareTo(CopyJob _other) {
    SimpleCopyJob other = (SimpleCopyJob) _other;
    if (highPriority != other.highPriority) {
      return highPriority ? -1 : 1;
    } else if (ord < other.ord) {
      // let earlier merges run to completion first
      return -1;
    } else if (ord > other.ord) {
      // let earlier merges run to completion first
      return 1;
    } else {
      return 0;
    }
  }

  /** Do an iota of work; returns true if all copying is done */
  public synchronized boolean visit() throws IOException {
    if (exc != null) {
      // We were externally cancelled:
      return true;
    }
    if (current == null) {
      if (iter.hasNext() == false) {
        return true;
      }
      Map.Entry<String, FileMetaData> next = iter.next();
      FileMetaData metaData = next.getValue();
      String fileName = next.getKey();
      Iterator<RawFileChunk> rawFileChunkIterator;
      try {
        rawFileChunkIterator = primaryAddres.recvRawFile(fileName, 0, indexName);
      } catch (Throwable t) {
        cancel("exc during start", t);
        throw new NodeCommunicationException("exc during start", t);
      }
      current = new CopyOneFile(rawFileChunkIterator, dest, fileName, metaData, copyBuffer);
    }
    if (current.visit()) {
      // This file is done copying
      copiedFiles.put(current.name, current.tmpName);
      totBytesCopied += current.getBytesCopied();
      assert totBytesCopied <= totBytes
          : "totBytesCopied=" + totBytesCopied + " totBytes=" + totBytes;
      current = null;
      return false;
    }
    return false;
  }

  @Override
  public String toString() {
    return "SimpleCopyJob(ord="
        + ord
        + " "
        + reason
        + " highPriority="
        + highPriority
        + " files count="
        + files.size()
        + " bytesCopied="
        + totBytesCopied
        + " (of "
        + totBytes
        + ") filesCopied="
        + copiedFiles.size()
        + ")";
  }
}
