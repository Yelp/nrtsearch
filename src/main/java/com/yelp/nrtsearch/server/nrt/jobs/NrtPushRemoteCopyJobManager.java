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

import com.yelp.nrtsearch.server.nrt.NRTReplicaNode;
import com.yelp.nrtsearch.server.nrt.NrtDataManager;
import com.yelp.nrtsearch.server.nrt.state.NrtFileMetaData;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CopyJobManager implementation that receives NRT point notifications from the primary via gRPC but
 * downloads index files from S3 instead of streaming them from the primary. This reduces network
 * load on the primary while maintaining low-latency NRT point notifications.
 */
public class NrtPushRemoteCopyJobManager implements CopyJobManager {
  private static final Logger logger = LoggerFactory.getLogger(NrtPushRemoteCopyJobManager.class);

  private final NrtDataManager dataManager;
  private final NRTReplicaNode replicaNode;

  private volatile String mergePrimaryId;
  private volatile String mergeTimeString;

  public NrtPushRemoteCopyJobManager(NrtDataManager dataManager, NRTReplicaNode replicaNode) {
    this.dataManager = dataManager;
    this.replicaNode = replicaNode;
  }

  @Override
  public void setMergePreCopyMetadata(String primaryId, String timeString) {
    this.mergePrimaryId = primaryId;
    this.mergeTimeString = timeString;
  }

  @Override
  public void start() throws IOException {}

  @Override
  public CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    if (files != null) {
      return newMergePreCopyJob(reason, files, highPriority, onceDone);
    }
    NrtDataManager.PointStateWithTimestamp target = dataManager.getTargetPointState(null);
    checkNotStale(reason, target);
    return RemoteCopyJob.forNrtPoint(
        reason, target, dataManager, replicaNode, highPriority, onceDone);
  }

  /**
   * Make sure the point state from S3 is newer than the version this replica is searching. The
   * primary may notify a version before it is in S3, for example when it does not have
   * s3RefreshNotifyAfterUpload enabled, or from writeNRTPoint. Lucene only compares the notified
   * version with the current one, but the job copies whatever version is in S3, so without this
   * check the replica would re-apply the version it already has. That opens a second searcher with
   * the same version, which makes SearcherLifetimeManager.record() throw on every search.
   *
   * <p>Throws {@link NodeCommunicationException}, since that is how Lucene's {@link
   * org.apache.lucene.replicator.nrt.ReplicaNode#newNRTPoint} skips a point that cannot be copied.
   * The replica picks up the version on the next notification.
   */
  private void checkNotStale(String reason, NrtDataManager.PointStateWithTimestamp target)
      throws IOException {
    if (target.pointState() == null) {
      // RemoteCopyJob.forNrtPoint reports this
      return;
    }
    long remoteVersion = target.pointState().version;
    long currentVersion = replicaNode.getCurrentSearchingVersion();
    if (remoteVersion <= currentVersion) {
      String msg =
          String.format(
              "Skipping nrt point (%s), S3 point state version %d is not newer than current searching version %d",
              reason, remoteVersion, currentVersion);
      logger.warn(msg);
      // Lucene requires a non-null cause
      throw new NodeCommunicationException(msg, new IllegalStateException(msg));
    }
  }

  private CopyJob newMergePreCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    String primaryId = mergePrimaryId;
    String timeString = mergeTimeString;
    if (primaryId == null || timeString == null) {
      throw new IOException(
          "Missing S3 metadata for merge precopy (primaryId="
              + primaryId
              + ", timeString="
              + timeString
              + ")");
    }
    Map<String, NrtFileMetaData> nrtFiles = new HashMap<>();
    for (Map.Entry<String, FileMetaData> entry : files.entrySet()) {
      nrtFiles.put(entry.getKey(), new NrtFileMetaData(entry.getValue(), primaryId, timeString));
    }
    return new RemoteCopyJob(
        reason,
        null,
        null,
        null,
        dataManager,
        replicaNode,
        files,
        nrtFiles,
        highPriority,
        onceDone);
  }

  @Override
  public void finishNRTCopy(CopyJob copyJob) throws IOException {
    RemoteCopyJob.commitPointState(copyJob, dataManager);
  }

  @Override
  public void close() throws IOException {}
}
