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

import com.yelp.nrtsearch.server.config.IsolatedReplicaConfig;
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
 * CopyJobManager implementation that creates RemoteCopyJob instances to copy files from remote
 * storage (e.g., S3) instead of from a primary node. This is useful for recovering or replicating
 * data directly from remote storage.
 *
 * <p>This manager periodically polls the NrtDataManager for updates to the target point state and
 * initiates new copy jobs as needed.
 */
public class RemoteCopyJobManager implements CopyJobManager {
  private static final Logger logger = LoggerFactory.getLogger(NrtDataManager.class);

  private final int pollingIntervalSecond;
  private final IsolatedReplicaConfig isolatedReplicaConfig;
  private final NrtDataManager dataManager;
  private final NRTReplicaNode replicaNode;
  private final Thread updateThread;
  private final UpdateTask updateTask;

  public RemoteCopyJobManager(
      int pollingIntervalSecond,
      NrtDataManager dataManager,
      NRTReplicaNode replicaNode,
      IsolatedReplicaConfig isolatedReplicaConfig) {
    this.pollingIntervalSecond = pollingIntervalSecond;
    this.isolatedReplicaConfig = isolatedReplicaConfig;
    this.dataManager = dataManager;
    this.replicaNode = replicaNode;
    logger.info(
        "Starting RemoteCopyJobManager with polling interval of {}s", pollingIntervalSecond);
    updateTask = new UpdateTask();
    updateThread = new Thread(updateTask, "RemoteCopyJobManager-UpdateThread");
    updateThread.setDaemon(true);
  }

  @Override
  public void start() throws IOException {
    updateThread.start();
  }

  @Override
  public CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    if (files != null) {
      throw new IllegalArgumentException("RemoteCopyJobManager does not support merge precopy");
    }

    NrtDataManager.PointStateWithTimestamp targetPointStateWithTimestamp =
        dataManager.getTargetPointState(isolatedReplicaConfig);

    CopyState copyState = targetPointStateWithTimestamp.pointState().toCopyState();
    return new RemoteCopyJob(
        reason,
        targetPointStateWithTimestamp.pointState(),
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
    // If the copy job failed, we do not update the last known point state
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
  public void close() throws IOException {
    updateTask.setQuitting();
    updateThread.interrupt();
    try {
      updateThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Background task that periodically polls the NrtDataManager for updates to the target point
   * state and initiates new NRT points on the replica node as needed.
   */
  class UpdateTask implements Runnable {
    private volatile boolean quitting = false;

    public UpdateTask() {}

    public void setQuitting() {
      quitting = true;
    }

    @Override
    public void run() {
      while (!quitting) {
        try {
          NrtPointState targetPointState;
          long currentIndexVersion;
          targetPointState = dataManager.getTargetPointState(isolatedReplicaConfig).pointState();
          currentIndexVersion = replicaNode.getCurrentSearchingVersion();
          if (targetPointState.version > currentIndexVersion) {
            logger.info(
                "Advancing to new NRT point: {}, current version: {}",
                targetPointState,
                currentIndexVersion);
            replicaNode.newNRTPoint(targetPointState.primaryGen, targetPointState.version);
          }
        } catch (Exception e) {
          logger.error("Error when polling for new point state: {}", e.getMessage(), e);
        }

        try {
          Thread.sleep(pollingIntervalSecond * 1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
