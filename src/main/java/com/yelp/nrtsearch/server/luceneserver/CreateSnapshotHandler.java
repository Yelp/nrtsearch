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

import com.google.gson.JsonObject;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotRequest;
import com.yelp.nrtsearch.server.grpc.CreateSnapshotResponse;
import com.yelp.nrtsearch.server.grpc.SnapshotId;
import java.io.IOException;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;

public class CreateSnapshotHandler
    implements Handler<CreateSnapshotRequest, CreateSnapshotResponse> {
  @Override
  public CreateSnapshotResponse handle(
      IndexState indexState, CreateSnapshotRequest createSnapshotRequest) throws HandlerException {
    try {
      return createSnapshot(indexState, createSnapshotRequest);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public CreateSnapshotResponse createSnapshot(
      IndexState indexState, CreateSnapshotRequest createSnapshotRequest) throws IOException {
    indexState.verifyStarted();

    final ShardState shardState = indexState.getShard(0);

    if (!indexState.hasCommit()) {
      throw new RuntimeException("this index has no commits; please call commit first");
    }

    // nocommit not thread safe vs commitHandler?
    final boolean openSearcher = createSnapshotRequest.getOpenSearcher();
    IndexCommit c = shardState.snapshots.snapshot();
    IndexCommit tc = null;
    // TODO: get taxonomy snapshot working for PrimaryMode
    if (!shardState.isPrimary() && !shardState.isReplica()) {
      tc = shardState.taxoSnapshots.snapshot();
    }
    long stateGen = indexState.incRefLastCommitGen();

    JsonObject result = new JsonObject();
    CreateSnapshotResponse.Builder createSnapShotResponseBuilder =
        CreateSnapshotResponse.newBuilder();

    SegmentInfos sis = SegmentInfos.readCommit(shardState.origIndexDir, c.getSegmentsFileName());
    shardState.snapshotGenToVersion.put(c.getGeneration(), sis.getVersion());

    if (openSearcher) {
      // nocommit share w/ SearchHandler's method:
      // TODO: this "reverse-NRT" is silly ... we need a reader
      // pool somehow:
      SearcherTaxonomyManager.SearcherAndTaxonomy s2 = shardState.acquire();
      try {
        // This returns a new reference to us, which
        // is decRef'd in the finally clause after
        // search is done:
        long t0 = System.nanoTime();
        IndexReader r =
            DirectoryReader.openIfChanged((DirectoryReader) s2.searcher.getIndexReader(), c);
        IndexSearcher s = new IndexSearcher(r);
        try {
          shardState.slm.record(s);
        } finally {
          s.getIndexReader().decRef();
        }
        long t1 = System.nanoTime();
        result.addProperty("newSnapshotSearcherOpenMS", ((t1 - t0) / 1000000.0));
      } finally {
        shardState.release(s2);
      }
    }

    // TODO: suggest state?
    // nocommit must also snapshot snapshots state!?
    // hard to think about

    SnapshotId.Builder snapshotIdBuilder =
        SnapshotId.newBuilder().setIndexGen(c.getGeneration()).setStateGen(stateGen);
    if (!shardState.isPrimary() && !shardState.isReplica()) {
      snapshotIdBuilder.setTaxonomyGen(tc.getGeneration());
    }
    CreateSnapshotResponse.Builder builder =
        createSnapShotResponseBuilder
            .addAllIndexFiles(c.getFileNames())
            .addStateFiles(String.valueOf(stateGen))
            .setSnapshotId(snapshotIdBuilder.build());
    if (!shardState.isPrimary() && !shardState.isReplica()) {
      builder.addAllTaxonomyFiles(tc.getFileNames());
    }
    return builder.build();
  }

  public static String getSnapshotIdAsString(SnapshotId snapshotId) {
    return snapshotId.getIndexGen()
        + ":"
        + snapshotId.getTaxonomyGen()
        + ":"
        + snapshotId.getStateGen();
  }
}
