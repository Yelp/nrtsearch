/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package org.apache.platypus.server.luceneserver;

import org.apache.platypus.server.grpc.ReleaseSnapshotRequest;
import org.apache.platypus.server.grpc.ReleaseSnapshotResponse;
import org.apache.platypus.server.grpc.SnapshotId;

import java.io.IOException;

import static org.apache.platypus.server.luceneserver.CreateSnapshotHandler.getSnapshotIdAsString;

public class ReleaseSnapshotHandler implements Handler<ReleaseSnapshotRequest, ReleaseSnapshotResponse> {
    @Override
    public ReleaseSnapshotResponse handle(IndexState indexState, ReleaseSnapshotRequest releaseSnapshotRequest) throws HandlerException {
        final ShardState shardState = indexState.getShard(0);
        final IndexState.Gens gens = new IndexState.Gens(getSnapshotIdAsString(releaseSnapshotRequest.getSnapshotId()), "id");
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
            indexState.decRef(gens.stateGen);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ReleaseSnapshotResponse.newBuilder().setSuccess(true).build();
    }
}
