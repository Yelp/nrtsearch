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
package com.yelp.nrtsearch.server.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.yelp.nrtsearch.server.grpc.CopyFiles;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import com.yelp.nrtsearch.server.nrt.NRTReplicaNode;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.ProtoMessagePrinter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyFilesHandler extends Handler<CopyFiles, TransferStatus> {
  private static final Logger logger = LoggerFactory.getLogger(CopyFilesHandler.class);
  private static final long CHECK_SLEEP_TIME_MS = 10;
  private static final int CHECKS_PER_STATUS_MESSAGE = 3000; // at least 30s between status messages
  private final boolean verifyIndexId;

  public CopyFilesHandler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public void handle(CopyFiles request, StreamObserver<TransferStatus> responseObserver) {
    try {
      IndexStateManager indexStateManager = getIndexStateManager(request.getIndexName());
      checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

      IndexState indexState = indexStateManager.getCurrent();
      // we need to send multiple responses to client from this method
      handle(indexState, request, responseObserver);
    } catch (Exception e) {
      String requestStr;
      try {
        requestStr = ProtoMessagePrinter.omittingInsignificantWhitespace().print(request);
      } catch (InvalidProtocolBufferException ignored) {
        // Ignore as invalid proto would have thrown an exception earlier
        requestStr = request.toString();
      }
      logger.warn(String.format("Error handling copyFiles request: %s", requestStr), e);
      if (e instanceof StatusRuntimeException) {
        responseObserver.onError(e);
      } else {
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "Error on copyFiles for primaryGen: %s, for index: %s",
                        request.getPrimaryGen(), request.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    }
  }

  private void handle(
      IndexState indexState,
      CopyFiles copyFilesRequest,
      StreamObserver<TransferStatus> responseObserver)
      throws Exception {
    String indexName = copyFilesRequest.getIndexName();
    ShardState shardState = indexState.getShard(0);

    if (!shardState.isReplica()) {
      throw new IllegalArgumentException(
          "index \"" + indexName + "\" is not a replica or was not started yet");
    }

    if (!isValidMagicHeader(copyFilesRequest.getMagicNumber())) {
      throw new RuntimeException("RecvCopyStateHandler invoked with Invalid Magic Number");
    }

    long primaryGen = copyFilesRequest.getPrimaryGen();
    // these are the files that the remote (primary) wants us to copy
    Map<String, FileMetaData> files =
        NRTReplicaNode.readFilesMetaData(copyFilesRequest.getFilesMetadata());

    AtomicBoolean finished = new AtomicBoolean();
    CopyJob job;
    long startNS = System.nanoTime();
    try {
      job = shardState.nrtReplicaNode.launchPreCopyFiles(finished, primaryGen, files);
    } catch (IOException e) {
      responseObserver.onNext(
          TransferStatus.newBuilder()
              .setMessage("replica failed to launchPreCopyFiles" + files.keySet())
              .setCode(TransferStatusCode.Failed)
              .build());
      // called must set; //responseObserver.onError(e);
      throw new RuntimeException(e);
    }

    // we hold open this request, only finishing/closing once our copy has finished, so primary
    // knows when we finished
    int sendStatusCounter = 0;
    while (true) {
      // nocommit don't poll!  use a condition...
      if (finished.get()) {
        responseObserver.onNext(
            TransferStatus.newBuilder()
                .setMessage("replica is done copying files.." + files.keySet())
                .setCode(TransferStatusCode.Done)
                .build());
        responseObserver.onCompleted();
        break;
      }
      try {
        Thread.sleep(CHECK_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        responseObserver.onNext(
            TransferStatus.newBuilder()
                .setMessage("replica failed to copy files..." + files.keySet())
                .setCode(TransferStatusCode.Failed)
                .build());
        // caller must set; //responseObserver.onError(e);
        throw new RuntimeException(e);
      }
      sendStatusCounter++;
      if (sendStatusCounter == CHECKS_PER_STATUS_MESSAGE) {
        responseObserver.onNext(
            TransferStatus.newBuilder()
                .setMessage("replica is copying files..." + files.keySet())
                .setCode(TransferStatusCode.Ongoing)
                .build());
        sendStatusCounter = 0;
      }
    }

    // record metrics for merge copy
    if (job.getFailed()) {
      NrtMetrics.nrtMergeFailure.labelValues(indexName).inc();
    } else {
      NrtMetrics.nrtMergeTime
          .labelValues(indexName)
          .observe((System.nanoTime() - startNS) / 1000000.0);
      NrtMetrics.nrtMergeSize.labelValues(indexName).observe(job.getTotalBytesCopied());
    }
  }
}
