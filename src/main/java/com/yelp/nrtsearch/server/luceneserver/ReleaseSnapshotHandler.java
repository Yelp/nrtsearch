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

import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.ReleaseSnapshotResponse;
import com.yelp.nrtsearch.server.luceneserver.handler.Handler;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReleaseSnapshotHandler
    extends Handler<ReleaseSnapshotRequest, ReleaseSnapshotResponse> {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseSnapshotHandler.class);

  public ReleaseSnapshotHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      ReleaseSnapshotRequest releaseSnapshotRequest,
      StreamObserver<ReleaseSnapshotResponse> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndex(releaseSnapshotRequest.getIndexName());
      ReleaseSnapshotResponse reply = handle(indexState, releaseSnapshotRequest);
      logger.info(String.format("CreateSnapshotHandler returned results %s", reply.toString()));
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn(
          String.format(
              "error while trying to releaseSnapshot for index %s",
              releaseSnapshotRequest.getIndexName()),
          e);
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(
                  String.format(
                      "error while trying to releaseSnapshot for index %s",
                      releaseSnapshotRequest.getIndexName()))
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private ReleaseSnapshotResponse handle(
      IndexState indexState, ReleaseSnapshotRequest releaseSnapshotRequest) {
    final ShardState shardState = indexState.getShard(0);
    final IndexState.Gens gens =
        new IndexState.Gens(
            CreateSnapshotHandler.getSnapshotIdAsString(releaseSnapshotRequest.getSnapshotId()));
    // SearcherLifetimeManager pruning thread will drop
    // the searcher (if it's old enough) next time it
    // wakes up:
    try {
      shardState.snapshots.release(gens.indexGen);
      shardState.writer.deleteUnusedFiles();
      shardState.snapshotGenToVersion.remove(gens.indexGen);
      if (!shardState.isPrimary() && !shardState.isReplica()) {
        shardState.taxoSnapshots.release(gens.taxoGen);
        shardState.taxoInternalWriter.deleteUnusedFiles();
      }

      long stateGen = gens.stateGen;
      if (indexState.getGenRefCounts().containsKey(stateGen)) {
        indexState.decRef(stateGen);
      } else {
        logger.warn("State gen {} is not held by a snapshot, skipping release", stateGen);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ReleaseSnapshotResponse.newBuilder().setSuccess(true).build();
  }
}
