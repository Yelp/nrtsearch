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
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
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

    NrtDataManager.PointStateWithTimestamp targetPointStateWithTimestamp =
        dataManager.getTargetPointState(null);
    NrtPointState pointState = targetPointStateWithTimestamp.pointState();
    if (pointState == null) {
      throw new IOException("No point state available from S3");
    }

    CopyState copyState = pointState.toCopyState();
    return new RemoteCopyJob(
        reason,
        pointState,
        targetPointStateWithTimestamp.timestamp(),
        copyState,
        dataManager,
        replicaNode,
        copyState.files(),
        highPriority,
        onceDone);
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
    if (copyJob.getFailed()) {
      return;
    }
    if (copyJob instanceof RemoteCopyJob remoteCopyJob) {
      NrtPointState pointState = remoteCopyJob.getPointState();
      Instant pointStateTimestamp = remoteCopyJob.getPointStateTimestamp();
      dataManager.setLastPointState(pointState, pointStateTimestamp);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Expected copyJob to be instance of RemoteCopyJob, got %s",
              copyJob.getClass().getName()));
    }
  }

  @Override
  public void close() throws IOException {}
}
