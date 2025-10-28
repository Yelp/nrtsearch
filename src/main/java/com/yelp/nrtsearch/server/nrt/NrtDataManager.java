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

import static com.yelp.nrtsearch.server.state.BackendGlobalState.getBaseIndexName;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.config.IsolatedReplicaConfig;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.monitoring.BootstrapMetrics;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
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
import java.time.Instant;
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

  // Set during restoreIfNeeded, then updated by setLastPointState, which only happens on replicas
  // using the isolated replica feature
  private volatile Instant lastPointTimestamp = null;

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

  /**
   * Represents a point state along with the timestamp it was uploaded to the remote backend.
   *
   * @param pointState NrtPointState object
   * @param timestamp Instant representing the time the point state was uploaded
   */
  public record PointStateWithTimestamp(NrtPointState pointState, Instant timestamp) {}

  @VisibleForTesting
  NrtPointState getLastPointState() {
    return lastPointState;
  }

  @VisibleForTesting
  Instant getLastPointTimestamp() {
    return lastPointTimestamp;
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
    restoreIfNeeded(shardDataDir, null);
  }

  /**
   * Restore the index data if it is available in the remote backend.
   *
   * @param shardDataDir Path to the shard index data directory
   * @param isolatedReplicaConfig configuration for isolated replica, or null if not using isolated
   *     replica
   * @throws IOException if an error occurs while restoring the index data
   */
  public void restoreIfNeeded(Path shardDataDir, IsolatedReplicaConfig isolatedReplicaConfig)
      throws IOException {
    restoreIfNeeded(shardDataDir, isolatedReplicaConfig, false);
  }

  /**
   * Restore the index data if it is available in the remote backend.
   *
   * @param shardDataDir Path to the shard index data directory
   * @param isolatedReplicaConfig configuration for isolated replica, or null if not using isolated
   *     replica
   * @param metadataOnly if true, only restore metadata (segments file) without downloading index
   *     files. Used for remote-only mode where files are streamed from S3.
   * @throws IOException if an error occurs while restoring the index data
   */
  public void restoreIfNeeded(
      Path shardDataDir, IsolatedReplicaConfig isolatedReplicaConfig, boolean metadataOnly)
      throws IOException {
    if (restoreIndex == null) {
      return;
    }
    if (restoreIndex.getDeleteExistingData()) {
      FileUtils.deleteAllFilesInDir(shardDataDir);
    }
    if (hasRestoreData()) {
      logger.info("Restoring index data for service: {}, index: {}", serviceName, indexIdentifier);
      RemoteBackend.InputStreamWithTimestamp pointStateWithTimestamp =
          remoteBackend.downloadPointState(
              serviceName,
              indexIdentifier,
              createUpdateIntervalContext(isolatedReplicaConfig, null));
      InputStream pointStateStream = pointStateWithTimestamp.inputStream();
      byte[] pointStateBytes = pointStateStream.readAllBytes();
      NrtPointState pointState = RemoteUtils.pointStateFromUtf8(pointStateBytes);

      logger.info(
          "Point state: {}, timestamp: {}", pointState, pointStateWithTimestamp.timestamp());

      long start = System.nanoTime();
      try {
        if (metadataOnly) {
          logger.info(
              "Remote-only mode: skipping file download, only writing segments file for index: {}",
              indexIdentifier);
          writeSegmentsFile(pointState.infosBytes, pointState.gen, shardDataDir);
        } else {
          remoteBackend.downloadIndexFiles(
              serviceName, indexIdentifier, shardDataDir, pointState.files);
          writeSegmentsFile(pointState.infosBytes, pointState.gen, shardDataDir);
        }
      } finally {
        double timeSpentMs = (System.nanoTime() - start) / 1_000_000.0;
        logger.info(
            "Restored index data for service: {}, index: {} in {}ms",
            serviceName,
            indexIdentifier,
            timeSpentMs);
        // Record in seconds, required by prometheus {@link
        // https://prometheus.io/docs/instrumenting/writing_exporters/#naming}: Metrics must use
        // base units (e.g. seconds, bytes) and leave converting them to something more readable to
        // graphing tools.
        BootstrapMetrics.dataRestoreTimer
            .labelValues(getBaseIndexName(indexIdentifier), indexIdentifier)
            .set(timeSpentMs / 1_000.0);
      }

      lastPointState = pointState;
      lastPointTimestamp = pointStateWithTimestamp.timestamp();
      NrtMetrics.indexTimestampSec
          .labelValues(getBaseIndexName(indexIdentifier))
          .set(lastPointTimestamp.getEpochSecond());
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
   * Create an update interval context from the isolated replica configuration. If the isolated
   * replica configuration is null or not enabled, null is returned.
   *
   * @param isolatedReplicaConfig configuration for isolated replica, or null if not using isolated
   *     replica
   * @param currentTimestamp timestamp of the currently loaded index version, or null if no loaded
   *     version
   * @return RemoteBackend.UpdateIntervalContext object with appropriate update interval, or null
   */
  @VisibleForTesting
  static RemoteBackend.UpdateIntervalContext createUpdateIntervalContext(
      IsolatedReplicaConfig isolatedReplicaConfig, Instant currentTimestamp) {
    if (isolatedReplicaConfig == null || !isolatedReplicaConfig.isEnabled()) {
      return null;
    }
    int updateIntervalSeconds =
        freshnessToUpdateIntervalSeconds(isolatedReplicaConfig.getFreshnessTargetSeconds());
    return new RemoteBackend.UpdateIntervalContext(
        updateIntervalSeconds,
        currentTimestamp,
        isolatedReplicaConfig.getFreshnessTargetOffsetSeconds());
  }

  /**
   * Convert freshness target interval to update interval in seconds. The update interval is used to
   * determine which index version to download from the remote backend. The update interval is half
   * the freshness target interval, with a minimum of 1 second. If the freshness target interval is
   * 0 or negative, the update interval is 0, which means no interval and the latest version is
   * always returned.
   *
   * @param freshnessTargetInterval Target for how fresh the replica index data should be in
   *     seconds, must be >= 0, 0 means as fresh as possible
   * @return update interval in seconds
   */
  @VisibleForTesting
  static int freshnessToUpdateIntervalSeconds(int freshnessTargetInterval) {
    if (freshnessTargetInterval <= 0) {
      return 0;
    }
    // Use half the target interval as the update interval, with a minimum of 1 second. This is
    // needed for cases where on update is at the start of the freshness interval, and the next
    // update is at the end. Using half the interval ensures that the second update will not be
    // skipped, violating the freshness target.
    return Math.max(1, freshnessTargetInterval / 2);
  }

  /**
   * Download a single index file from the remote backend.
   *
   * @param fileName file name to download
   * @param fileMetaData file metadata
   * @return input stream of the index file
   * @throws IOException on error downloading the file
   */
  public InputStream downloadIndexFile(String fileName, NrtFileMetaData fileMetaData)
      throws IOException {
    return remoteBackend.downloadIndexFile(serviceName, indexIdentifier, fileName, fileMetaData);
  }

  /**
   * Get the file metadata from the last restored point state. This is used to create S3Directory
   * instances that can read index files directly from S3.
   *
   * @return map of file names to their metadata, or empty map if no restore has been done
   */
  public Map<String, NrtFileMetaData> getRestoredFileMetadata() {
    NrtPointState pointState = lastPointState;
    if (pointState == null) {
      return Map.of();
    }
    return Map.copyOf(pointState.files);
  }

  /**
   * Get the service name.
   *
   * @return service name
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Get the index identifier.
   *
   * @return index identifier
   */
  public String getIndexIdentifier() {
    return indexIdentifier;
  }

  /**
   * Get the remote backend.
   *
   * @return remote backend
   */
  public RemoteBackend getRemoteBackend() {
    return remoteBackend;
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

  /**
   * Get the target point state that should be loaded from the remote backend. This is used by the
   * isolated replica feature to determine when and index update is needed. It currently returns the
   * latest point state from the remote backend.
   *
   * @param isolatedReplicaConfig configuration for isolated replica, or null if not using isolated
   *     replicas
   * @return PointStateWithTimestamp object containing the target point state and its timestamp
   * @throws IOException if an error occurs while downloading the point state
   */
  public PointStateWithTimestamp getTargetPointState(IsolatedReplicaConfig isolatedReplicaConfig)
      throws IOException {
    RemoteBackend.InputStreamWithTimestamp inputStreamWithTimestamp =
        remoteBackend.downloadPointState(
            serviceName,
            indexIdentifier,
            createUpdateIntervalContext(isolatedReplicaConfig, lastPointTimestamp));
    // No update is needed based on the freshness target, return the current point state
    if (inputStreamWithTimestamp == null) {
      synchronized (this) {
        return new PointStateWithTimestamp(lastPointState, lastPointTimestamp);
      }
    }
    InputStream pointStateStream = inputStreamWithTimestamp.inputStream();
    byte[] pointStateBytes = pointStateStream.readAllBytes();
    return new PointStateWithTimestamp(
        RemoteUtils.pointStateFromUtf8(pointStateBytes), inputStreamWithTimestamp.timestamp());
  }

  /**
   * Set the last point state. This is used by the isolated replica feature to update the last point
   * state after the copy job is complete.
   *
   * @param pointState NrtPointState object representing the last point state
   * @param timestamp Instant representing the time the point state was uploaded
   */
  public synchronized void setLastPointState(NrtPointState pointState, Instant timestamp) {
    logger.info("Setting last point state: {}, timestamp: {}", pointState, timestamp);
    if (lastPointState == null || (pointState.version > lastPointState.version)) {
      this.lastPointState = pointState;
      this.lastPointTimestamp = timestamp;
      NrtMetrics.indexTimestampSec
          .labelValues(getBaseIndexName(indexIdentifier))
          .set(lastPointTimestamp.getEpochSecond());
    } else {
      logger.info(
          "Not setting last point state, existing version is later. Existing version: {}, new version: {}",
          lastPointState.version,
          pointState.version);
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
