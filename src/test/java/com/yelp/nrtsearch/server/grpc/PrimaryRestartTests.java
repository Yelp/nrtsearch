/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager.PruneByAge;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrimaryRestartTests {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testReplication() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexDataLocationType.REMOTE)
            .build();
    replicaServer.verifySimpleDocs("test_index", 3);

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocs("test_index", 5);
    replicaServer.verifySimpleDocs("test_index", 5);
  }

  @Test
  public void testPrimaryRestartReplication() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig("discoveryFileUpdateIntervalMs: 1000")
            .build();

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    replicaServer.registerWithPrimary("test_index");

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // primary index version not greater than local version on replica
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    primaryServer.addSimpleDocs("test_index", 9);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // primary version is greater, but conflicting segment files prevent replica from updating
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8, 9);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
  }

  @Test
  public void testPrimaryRestartReplicationFilterIncompatible() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "discoveryFileUpdateIntervalMs: 1000",
                    "filterIncompatibleSegmentReaders: true"))
            .build();

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    replicaServer.registerWithPrimary("test_index");

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // primary index version not greater than local version on replica
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);

    primaryServer.addSimpleDocs("test_index", 9);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // primary version is greater and conflicting segments are filtered
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8, 9);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 6, 7, 8, 9);
  }

  @Test
  public void testPreviousReplicaSearcher() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "discoveryFileUpdateIntervalMs: 1000",
                    "filterIncompatibleSegmentReaders: true"))
            .build();

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
    long previousSearcherVersion1 = getCurrentSearcherVersion(replicaServer);

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);
    long previousSearcherVersion2 = getCurrentSearcherVersion(replicaServer);

    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    replicaServer.registerWithPrimary("test_index");

    // advance primary index version past replica
    primaryServer.addSimpleDocs("test_index", 9);
    primaryServer.refresh("test_index");
    primaryServer.addSimpleDocs("test_index", 10);
    primaryServer.refresh("test_index");
    primaryServer.addSimpleDocs("test_index", 11);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    primaryServer.addSimpleDocs("test_index", 12);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testCleanupPreviousSearcher() throws IOException, InterruptedException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "discoveryFileUpdateIntervalMs: 1000",
                    "filterIncompatibleSegmentReaders: true"))
            .build();

    primaryServer.addSimpleDocs("test_index", 4, 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
    long previousSearcherVersion1 = getCurrentSearcherVersion(replicaServer);
    List<LeafReaderContext> previousLeaves1 =
        getVersionLeaves(replicaServer, previousSearcherVersion1);

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);
    long previousSearcherVersion2 = getCurrentSearcherVersion(replicaServer);
    List<LeafReaderContext> previousLeaves2 =
        getVersionLeaves(replicaServer, previousSearcherVersion2);

    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    replicaServer.registerWithPrimary("test_index");

    // advance primary index version past replica
    primaryServer.addSimpleDocs("test_index", 9);
    primaryServer.refresh("test_index");
    primaryServer.addSimpleDocs("test_index", 10);
    primaryServer.refresh("test_index");
    primaryServer.addSimpleDocs("test_index", 11);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    long currentSearcherVersion1 = getCurrentSearcherVersion(replicaServer);
    List<LeafReaderContext> currentLeaves1 =
        getVersionLeaves(replicaServer, currentSearcherVersion1);

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    primaryServer.addSimpleDocs("test_index", 12);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    long currentSearcherVersion2 = getCurrentSearcherVersion(replicaServer);
    List<LeafReaderContext> currentLeaves2 =
        getVersionLeaves(replicaServer, currentSearcherVersion2);

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    verifySegmentReaderStatus(previousLeaves1, Set.of("_0", "_1"), Collections.emptySet());
    verifySegmentReaderStatus(previousLeaves2, Set.of("_0", "_1", "_2"), Collections.emptySet());
    verifySegmentReaderStatus(
        currentLeaves1, Set.of("_0", "_1", "_2", "_3"), Collections.emptySet());
    verifySegmentReaderStatus(
        currentLeaves2, Set.of("_0", "_1", "_2", "_3", "_4"), Collections.emptySet());

    Thread.sleep(3000);
    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .slm
        .prune(new PruneByAge(1));

    verifySegmentReaderStatus(previousLeaves1, Set.of("_0"), Set.of("_1"));
    verifySegmentReaderStatus(previousLeaves2, Set.of("_0"), Set.of("_1", "_2"));
    verifySegmentReaderStatus(
        currentLeaves1, Set.of("_0", "_1", "_2", "_3"), Collections.emptySet());
    verifySegmentReaderStatus(
        currentLeaves2, Set.of("_0", "_1", "_2", "_3", "_4"), Collections.emptySet());
  }

  /**
   * Verifies that replicas with filterIncompatibleSegmentReaders enabled can handle a primary
   * restart that loses uncommitted doc values updates. Without the backward-generation check, the
   * old SegmentReader's shared core/segDocValues state is reused for a segment whose fieldInfosGen
   * has gone backwards, causing IllegalStateException when accessing numeric doc values.
   */
  @Test
  public void testPrimaryRestartDocValuesUpdateFilter() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    // Commit base documents so they exist in a committed segment
    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "discoveryFileUpdateIntervalMs: 1000",
                    "filterIncompatibleSegmentReaders: true"))
            .build();

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3);

    // Apply a doc values update to field1 for doc id=1 without committing.
    // This advances fieldInfosGen for the committed segment on the primary.
    primaryServer.addDocs(
        List.of(
            AddDocumentRequest.newBuilder()
                .setIndexName("test_index")
                .setRequestType(IndexingRequestType.UPDATE_DOC_VALUES)
                .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
                .putFields("field1", MultiValuedField.newBuilder().addValue("999").build())
                .build())
            .stream());
    // NRT refresh propagates the update to replica without committing
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // Restart primary: loses the uncommitted doc values update, fieldInfosGen rolls back
    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);

    replicaServer.registerWithPrimary("test_index");

    // Advance primary version past the replica so it syncs and refreshes readers
    primaryServer.addSimpleDocs("test_index", 4);
    primaryServer.refresh("test_index");
    primaryServer.addSimpleDocs("test_index", 5);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // Without the backward-generation filter fix, the replica throws
    // IllegalStateException("unexpected docvalues type NUMERIC for field 'field1' ...")
    // when retrieving field1 for docs 1-3 whose segment reader has stale shared state.
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5);
  }

  /**
   * Tests that the replica correctly handles the gen-reuse case: after a primary restart, the
   * generation counter resets to the same value used before the restart, so a new doc values update
   * produces the same fieldInfosGen as the pre-restart update. The simple "gen &lt; old gen" check
   * cannot detect this; the primaryGen-gated SegmentCommitInfo ID check must catch it.
   */
  @Test
  public void testPrimaryRestartDocValuesUpdateGenReuse() throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withWriteDiscoveryFile(true)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    // Commit base documents so they exist in a committed segment
    primaryServer.addSimpleDocs("test_index", 1, 2, 3);
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(
                String.join(
                    "\n",
                    "discoveryFileUpdateIntervalMs: 1000",
                    "filterIncompatibleSegmentReaders: true"))
            .build();

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3);

    // Apply update U1: advances fieldInfosGen to 1 (from -1) without committing.
    primaryServer.addDocs(
        List.of(
            AddDocumentRequest.newBuilder()
                .setIndexName("test_index")
                .setRequestType(IndexingRequestType.UPDATE_DOC_VALUES)
                .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
                .putFields("field1", MultiValuedField.newBuilder().addValue("999").build())
                .build())
            .stream());
    primaryServer.refresh("test_index");
    replicaServer.waitForReplication("test_index");
    // Replica reader now has fieldInfosGen=1 for this segment.

    // Restart primary: rolls fieldInfosGen back to -1 (loses U1).
    primaryServer.restart();
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3);

    // Apply U2 on the restarted primary BEFORE any NRT refresh and BEFORE the replica
    // re-registers. nextWriteFieldInfosGen resets to 1 after rollback, so U2 also gets
    // fieldInfosGen=1 — same number as U1 but with a different SegmentCommitInfo ID.
    // We use a field value of id*3=6 for doc id=2 so that verifySimpleDocIds passes
    // when the fresh reader is opened correctly.
    primaryServer.addDocs(
        List.of(
            AddDocumentRequest.newBuilder()
                .setIndexName("test_index")
                .setRequestType(IndexingRequestType.UPDATE_DOC_VALUES)
                .putFields("id", MultiValuedField.newBuilder().addValue("2").build())
                .putFields("field1", MultiValuedField.newBuilder().addValue("6").build())
                .build())
            .stream());

    // Register the replica BEFORE the first post-restart refresh. No intermediate gen=-1
    // NRT point has been sent, so the replica will go directly from gen=1 (U1) to gen=1
    // (U2) when the refresh below fires — triggering the gen-reuse scenario.
    replicaServer.registerWithPrimary("test_index");

    // Flush U2 to create an NRT point with fieldInfosGen=1 (gen-reuse!). The primary
    // sends this to the registered replica.
    primaryServer.refresh("test_index");
    // Advance primary version to ensure the replica copies and refreshes its readers.
    primaryServer.addSimpleDocs("test_index", 4);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");

    // Without the primaryGen-gated SCI ID check, the replica reuses the stale reader from
    // U1 (fieldInfosGen=1 == fieldInfosGen=1, so the backward-gen "<" check is false).
    // The stale reader returns field1=999 for doc id=1 (U1 set it; U1 was rolled back so
    // the correct committed value is id*3=3). verifySimpleDocIds fails with expected 3 but
    // was 999. With the fix, a fresh reader is opened and field1=3 is returned correctly.
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }

  private List<LeafReaderContext> getVersionLeaves(TestServer server, long version)
      throws IOException {
    IndexSearcher searcher =
        server.getGlobalState().getIndexOrThrow("test_index").getShard(0).slm.acquire(version);
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    server.getGlobalState().getIndexOrThrow("test_index").getShard(0).slm.release(searcher);
    return leaves;
  }

  private long getCurrentSearcherVersion(TestServer server) {
    return server
        .getClient()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName("test_index")
                .setQuery(Query.newBuilder().build())
                .build())
        .getSearchState()
        .getSearcherVersion();
  }

  private void verifySimpleDocIdsForVersion(
      TestServer server, long version, String indexName, int... ids) {
    Set<Integer> uniqueIds = new HashSet<>();
    for (int id : ids) {
      uniqueIds.add(id);
    }
    SearchResponse response =
        server
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(indexName)
                    .addAllRetrieveFields(TestServer.simpleFieldNames)
                    .setTopHits(uniqueIds.size() + 1)
                    .setStartHit(0)
                    .setVersion(version)
                    .build());
    assertEquals(uniqueIds.size(), response.getHitsCount());
    Set<Integer> uniqueHitIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      int id = Integer.parseInt(hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("field1").getFieldValue(0).getIntValue();
      int f2 = Integer.parseInt(hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      assertEquals(id * 3, f1);
      assertEquals(id * 5, f2);
      uniqueHitIds.add(id);
    }
    assertEquals(uniqueIds, uniqueHitIds);
  }

  private void verifySegmentReaderStatus(
      List<LeafReaderContext> leaves,
      Set<String> expectedOpenSegments,
      Set<String> expectedClosedSegments) {
    Set<String> closedSegments = new HashSet<>();
    Set<String> openSegments = new HashSet<>();
    for (LeafReaderContext context : leaves) {
      String name = ((SegmentReader) context.reader()).getSegmentName();
      if (context.reader().getRefCount() == 0) {
        closedSegments.add(name);
      } else {
        openSegments.add(name);
      }
    }
    assertEquals(expectedClosedSegments, closedSegments);
    assertEquals(expectedOpenSegments, openSegments);
  }
}
