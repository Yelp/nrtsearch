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

import com.yelp.nrtsearch.server.grpc.CopyFiles;
import com.yelp.nrtsearch.server.grpc.TransferStatus;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.monitoring.NrtMetrics;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;

public class CopyFilesHandler implements Handler<CopyFiles, TransferStatus> {
  private static final long CHECK_SLEEP_TIME_MS = 10;
  private static final int CHECKS_PER_STATUS_MESSAGE = 3000; // at least 30s between status messages

  @Override
  public void handle(
      IndexState indexState,
      CopyFiles copyFilesRequest,
      StreamObserver<TransferStatus> responseObserver)
      throws Exception {
    String indexName = copyFilesRequest.getIndexName();
    ShardState shardState = indexState.getShard(0);

    if (shardState.isReplica() == false) {
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
      NrtMetrics.nrtMergeFailure.labels(indexName).inc();
    } else {
      NrtMetrics.nrtMergeTime.labels(indexName).observe((System.nanoTime() - startNS) / 1000000.0);
      NrtMetrics.nrtMergeSize.labels(indexName).observe(job.getTotalBytesCopied());
    }
  }

  @Override
  public TransferStatus handle(IndexState indexState, CopyFiles protoRequest)
      throws HandlerException {
    throw new UnsupportedOperationException("This method is in not implemented for this class");
  }
}
