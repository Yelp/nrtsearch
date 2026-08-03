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
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import java.io.IOException;
import java.time.Instant;
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

  public NrtPushRemoteCopyJobManager(NrtDataManager dataManager, NRTReplicaNode replicaNode) {
    this.dataManager = dataManager;
    this.replicaNode = replicaNode;
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
      throw new IllegalArgumentException(
          "NrtPushRemoteCopyJobManager does not support merge precopy");
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
