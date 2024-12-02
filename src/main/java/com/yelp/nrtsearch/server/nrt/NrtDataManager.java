/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.utils.FileUtils;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the NRT data for a single shard. This includes uploading new index files to the remote
 * backend and downloading the latest index files from the remote backend.
 *
 * <p>Enqueued upload tasks are processed by a background thread. The upload manager thread will
 * upload the diff of the new index files and the last committed index files to the remote backend.
 * The upload manager thread will also upload the new point state to the remote backend.
 *
 * <p>There is only one active upload and one pending upload at a time. If a new upload is enqueued
 * while there is already a pending upload, the new upload will be merged with the pending upload.
 */
public class NrtDataManager implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(NrtDataManager.class);
  private final String serviceName;
  private final String ephemeralId;
  private final String indexIdentifier;
  private final RemoteBackend remoteBackend;
  private final RestoreIndex restoreIndex;
  private final boolean remoteCommit;

  // Set during startUploadManager
  private NRTPrimaryNode primaryNode;
  private Path shardDataDir;
  private UploadManagerThread uploadManagerThread = null;

  // Set during restoreIfNeeded, then only accessed by UploadManagerThread
  private volatile NrtPointState lastPointState = null;

  // use synchronized access
  private UploadTask currentUploadTask = null;
  private UploadTask nextUploadTask = null;
  private boolean closed = false;

  /**
   * Represents an upload task that is enqueued to be processed by the UploadManagerThread.
   *
   * @param copyState CopyState to upload
   * @param watchers List of RefreshUploadFuture objects to notify when the upload is complete
   */
  record UploadTask(CopyState copyState, List<RefreshUploadFuture> watchers) {}

  /**
   * Create a new NrtDataManager.
   *
   * @param serviceName Name of the service
   * @param indexIdentifier Unique identifier for the index
   * @param ephemeralId Ephemeral ID for the node
   * @param remoteBackend Remote backend to use for uploading and downloading index files
   * @param restoreIndex RestoreIndex object to use for restoring index files, or null if no restore
   * @param remoteCommit Whether to commit to the remote backend
   */
  public NrtDataManager(
      String serviceName,
      String indexIdentifier,
      String ephemeralId,
      RemoteBackend remoteBackend,
      RestoreIndex restoreIndex,
      boolean remoteCommit) {
    this.serviceName = serviceName;
    this.ephemeralId = ephemeralId;
    this.indexIdentifier = indexIdentifier;
    this.remoteBackend = remoteBackend;
    this.restoreIndex = restoreIndex;
    this.remoteCommit = remoteCommit;
  }

  @VisibleForTesting
  NrtPointState getLastPointState() {
    return lastPointState;
  }

  @VisibleForTesting
  synchronized UploadTask getCurrentUploadTask() {
    return currentUploadTask;
  }

  @VisibleForTesting
  synchronized UploadTask getNextUploadTask() {
    return nextUploadTask;
  }

  @VisibleForTesting
  void startWithoutThread(NRTPrimaryNode primaryNode, Path shardDataDir) {
    this.primaryNode = primaryNode;
    this.shardDataDir = shardDataDir;
  }

  /**
   * Start the upload manager thread.
   *
   * @param primaryNode NRTPrimaryNode to use for releasing CopyState objects
   * @param shardDataDir Path to the shard index data directory
   */
  public void startUploadManager(NRTPrimaryNode primaryNode, Path shardDataDir) {
    if (uploadManagerThread != null) {
      throw new IllegalStateException("Upload manager already started");
    }
    this.primaryNode = primaryNode;
    this.shardDataDir = shardDataDir;
    uploadManagerThread = new UploadManagerThread();
    uploadManagerThread.start();
    logger.info("Upload manager started");
  }

  /**
   * Check if there is restore data available in the remote backend.
   *
   * @return true if restore data is available, false otherwise
   * @throws IOException if an error occurs while checking for restore data
   */
  public boolean hasRestoreData() throws IOException {
    return restoreIndex != null
        && remoteBackend.exists(
            serviceName, indexIdentifier, RemoteBackend.IndexResourceType.POINT_STATE);
  }

  /**
   * Check if remote index data commit should be done.
   *
   * @return true if remote commit should be done, false otherwise
   */
  public boolean doRemoteCommit() {
    return remoteCommit;
  }

  /**
   * Restore the index data if it is available in the remote backend.
   *
   * @param shardDataDir Path to the shard index data directory
   * @throws IOException if an error occurs while restoring the index data
   */
  public void restoreIfNeeded(Path shardDataDir) throws IOException {
    if (restoreIndex == null) {
      return;
    }
    if (restoreIndex.getDeleteExistingData()) {
      FileUtils.deleteAllFilesInDir(shardDataDir);
    }

    if (hasRestoreData()) {
      logger.info("Restoring index data for service: {}, index: {}", serviceName, indexIdentifier);
      InputStream pointStateStream = remoteBackend.downloadPointState(serviceName, indexIdentifier);
      byte[] pointStateBytes = pointStateStream.readAllBytes();
      NrtPointState pointState = RemoteUtils.pointStateFromUtf8(pointStateBytes);

      long start = System.nanoTime();
      try {
        remoteBackend.downloadIndexFiles(
            serviceName, indexIdentifier, shardDataDir, pointState.files);
        writeSegmentsFile(pointState.infosBytes, pointState.gen, shardDataDir);
      } finally {
        logger.info(
            "Restored index data for service: {}, index: {} in {}ms",
            serviceName,
            indexIdentifier,
            (System.nanoTime() - start) / 1_000_000.0);
      }

      lastPointState = pointState;
    }
  }

  @VisibleForTesting
  static void writeSegmentsFile(byte[] segmentBytes, long gen, Path shardDataDir)
      throws IOException {
    String segmentsFileName =
        IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen);
    Path segmentsFile = shardDataDir.resolve(segmentsFileName);
    try (OutputStream os = new FileOutputStream(segmentsFile.toFile())) {
      os.write(segmentBytes);
    }
  }

  /**
   * Enqueue an upload task to be processed by the UploadManagerThread.
   *
   * @param copyState CopyState of data to upload
   * @param watchers List of RefreshUploadFuture objects to notify when the upload is complete
   */
  public synchronized void enqueueUpload(CopyState copyState, List<RefreshUploadFuture> watchers) {
    if (!remoteCommit) {
      throw new IllegalStateException("Remote commit is not available for this configuration");
    }
    if (closed) {
      throw new IllegalStateException("NrtDataManager is closed");
    }
    UploadTask uploadTask = new UploadTask(copyState, watchers);
    if (currentUploadTask == null) {
      currentUploadTask = uploadTask;
      notifyAll();
    } else if (nextUploadTask == null) {
      nextUploadTask = uploadTask;
    } else {
      nextUploadTask = mergeTasks(nextUploadTask, uploadTask, primaryNode);
    }
  }

  @VisibleForTesting
  static UploadTask mergeTasks(UploadTask previous, UploadTask next, NRTPrimaryNode primaryNode) {
    List<RefreshUploadFuture> combinedWatchers = new ArrayList<>(previous.watchers);
    combinedWatchers.addAll(next.watchers);

    // make sure the latest version is used for the merged task
    CopyState taskCopyState;
    CopyState releaseCopyState;
    if (previous.copyState.version() <= next.copyState.version()) {
      taskCopyState = next.copyState;
      releaseCopyState = previous.copyState;
    } else {
      taskCopyState = previous.copyState;
      releaseCopyState = next.copyState;
    }

    // release previous CopyState to unref index files
    try {
      primaryNode.releaseCopyState(releaseCopyState);
    } catch (Throwable t) {
      logger.warn("Failed to release copy state", t);
    }

    return new UploadTask(taskCopyState, combinedWatchers);
  }

  /** Background thread that processes upload tasks. */
  class UploadManagerThread extends Thread {
    public UploadManagerThread() {
      super("UploadManagerThread");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        UploadTask task;
        synchronized (NrtDataManager.this) {
          while (!closed && currentUploadTask == null) {
            try {
              NrtDataManager.this.wait();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          if (closed) {
            return;
          }
          task = currentUploadTask;
        }

        try {
          if (isLaterVersion(task.copyState, lastPointState)) {
            logger.info(
                "Uploading new index files for service: {}, index: {}, version: {}",
                serviceName,
                indexIdentifier,
                task.copyState.version());
            Map<String, NrtFileMetaData> versionFiles = uploadDiff(task.copyState);
            NrtPointState pointState = new NrtPointState(task.copyState, versionFiles, ephemeralId);
            byte[] data = RemoteUtils.pointStateToUtf8(pointState);
            remoteBackend.uploadPointState(serviceName, indexIdentifier, pointState, data);
            lastPointState = pointState;
          } else {
            logger.info(
                "Later version committed, skipping. Committed version: {}, Task version: {}",
                lastPointState.version,
                task.copyState.version());
          }
          for (RefreshUploadFuture watcher : task.watchers) {
            watcher.setDone(null);
          }
        } catch (Throwable t) {
          for (RefreshUploadFuture watcher : task.watchers) {
            watcher.setDone(t);
          }
        } finally {
          // release current task index file references
          try {
            primaryNode.releaseCopyState(task.copyState);
          } catch (Throwable t) {
            logger.warn("Failed to release copy state", t);
          }
          // set next task as current task
          synchronized (NrtDataManager.this) {
            currentUploadTask = nextUploadTask;
            nextUploadTask = null;
          }
        }
      }
    }

    private boolean isLaterVersion(CopyState copyState, NrtPointState lastPointState) {
      return lastPointState == null || copyState.version() > lastPointState.version;
    }

    private Map<String, NrtFileMetaData> uploadDiff(CopyState copyState) throws IOException {
      Map<String, NrtFileMetaData> lastPointFiles =
          lastPointState != null ? lastPointState.files : Map.of();
      Map<String, NrtFileMetaData> currentPointFiles = new HashMap<>();
      Map<String, NrtFileMetaData> filesToUpload = new HashMap<>();

      for (Map.Entry<String, FileMetaData> entry : copyState.files().entrySet()) {
        String fileName = entry.getKey();
        FileMetaData fileMetaData = entry.getValue();

        NrtFileMetaData lastFileMetaData = lastPointFiles.get(fileName);
        if (lastFileMetaData != null && isSameFile(fileMetaData, lastFileMetaData)) {
          currentPointFiles.put(fileName, lastFileMetaData);
        } else {
          String timeString = TimeStringUtils.generateTimeStringSec();
          NrtFileMetaData nrtFileMetaData =
              new NrtFileMetaData(fileMetaData, ephemeralId, timeString);
          currentPointFiles.put(fileName, nrtFileMetaData);
          filesToUpload.put(fileName, nrtFileMetaData);
        }
      }
      logger.info("Uploading index files: {}", filesToUpload.keySet());
      remoteBackend.uploadIndexFiles(serviceName, indexIdentifier, shardDataDir, filesToUpload);
      return currentPointFiles;
    }

    @VisibleForTesting
    static boolean isSameFile(FileMetaData fileMetaData, NrtFileMetaData nrtFileMetaData) {
      return fileMetaData.length() == nrtFileMetaData.length
          && fileMetaData.checksum() == nrtFileMetaData.checksum
          && Arrays.equals(fileMetaData.header(), nrtFileMetaData.header)
          && Arrays.equals(fileMetaData.footer(), nrtFileMetaData.footer);
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      closed = true;
      notifyAll();
    }
    if (uploadManagerThread != null) {
      try {
        uploadManagerThread.join();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    synchronized (this) {
      if (currentUploadTask != null) {
        cleanupTask(currentUploadTask);
      }
      if (nextUploadTask != null) {
        cleanupTask(nextUploadTask);
      }
    }
  }

  private void cleanupTask(UploadTask task) {
    try {
      primaryNode.releaseCopyState(task.copyState);
    } catch (Throwable t) {
      logger.warn("Failed to release copy state", t);
    }
    Exception e = new IllegalStateException("NrtDataManager is closed");
    for (RefreshUploadFuture watcher : task.watchers) {
      watcher.setDone(e);
    }
  }
}
