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
package com.yelp.nrtsearch.server.nrt.jobs;

import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.InputStreamDataInput;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
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
import org.apache.lucene.replicator.nrt.StreamCopyOneFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CopyJob implementation that copies files from remote storage (S3) instead of from a primary
 * node. This is useful for recovering or replicating data directly from remote storage.
 */
public class RemoteCopyJob extends VisitableCopyJob {
  private static final Logger logger = LoggerFactory.getLogger(RemoteCopyJob.class);
  private static final byte[] COPY_BUFFER = new byte[1024 * 1024 * 4]; // 4MB

  private final NrtPointState pointState;
  private final Instant pointStateTimestamp;
  private final NrtDataManager dataManager;
  private final CopyState copyState;
  private Iterator<Map.Entry<String, FileMetaData>> iter;

  /**
   * Constructor.
   *
   * @param reason the reason for the copy
   * @param pointState the state for the nrt point index version
   * @param pointStateTimestamp timestamp of the point state
   * @param copyState the copy state
   * @param dataManager to download files from remote storage
   * @param dest the destination replica node
   * @param files the files to copy
   * @param highPriority if this is a high priority copy
   * @param onceDone callback when done
   * @throws IOException on I/O error
   */
  public RemoteCopyJob(
      String reason,
      NrtPointState pointState,
      Instant pointStateTimestamp,
      CopyState copyState,
      NrtDataManager dataManager,
      ReplicaNode dest,
      Map<String, FileMetaData> files,
      boolean highPriority,
      OnceDone onceDone)
      throws IOException {
    super(reason, files, dest, highPriority, onceDone);
    this.pointState = pointState;
    this.pointStateTimestamp = pointStateTimestamp;
    this.copyState = copyState;
    this.dataManager = dataManager;
  }

  /**
   * Get the NRT point state associated with this copy job.
   *
   * @return the NRT point state
   */
  public NrtPointState getPointState() {
    return pointState;
  }

  /**
   * Get the timestamp of the NRT point state associated with this copy job.
   *
   * @return the timestamp
   */
  public Instant getPointStateTimestamp() {
    return pointStateTimestamp;
  }

  @Override
  protected CopyOneFile newCopyOneFile(CopyOneFile prev) {
    // No state needs to be changed when transferring to a new job
    return prev;
  }

  @Override
  public void start() throws IOException {
    if (iter == null) {
      iter = toCopy.iterator();
      // This means we resumed an already in-progress copy; we do this one first:
      if (current != null) {
        totBytes += current.getFileMetaData().length();
      }
      for (Map.Entry<String, FileMetaData> ent : toCopy) {
        FileMetaData metaData = ent.getValue();
        totBytes += metaData.length();
      }

      // Send all file names / offsets up front to avoid ping-ping latency:
      try {
        dest.message(
            "RemoteCopyJob.init: done start files count="
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
    while (!visit())
      ;
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

    RemoteCopyJob other = (RemoteCopyJob) _other;
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
            "top: file copy done from Remote; took %.1f msec to copy %d bytes (%.2f MB/sec); now rename %d tmp files",
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

  /** Higher priority and then "first come first served" order. */
  @Override
  public int compareTo(CopyJob _other) {
    RemoteCopyJob other = (RemoteCopyJob) _other;
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
  @Override
  public synchronized boolean visit() throws IOException {
    if (exc != null) {
      // We were externally cancelled:
      return true;
    }
    if (current == null) {
      if (!iter.hasNext()) {
        return true;
      }
      Map.Entry<String, FileMetaData> next = iter.next();
      FileMetaData metaData = next.getValue();
      String fileName = next.getKey();
      InputStream remoteFileInputStream =
          dataManager.downloadIndexFile(fileName, pointState.files.get(fileName));
      current =
          new StreamCopyOneFile(
              new InputStreamDataInput(remoteFileInputStream),
              dest,
              fileName,
              metaData,
              COPY_BUFFER);
    }

    if (current.visit()) {
      // This file is done copying
      copiedFiles.put(current.getFileName(), current.getFileTmpName());
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
    return "RemoteCopyJob(ord="
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
