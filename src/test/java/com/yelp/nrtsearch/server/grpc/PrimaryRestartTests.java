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
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import java.io.IOException;
import java.util.Arrays;
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
    long previousSearcherVersion1 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);
    long previousSearcherVersion2 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();

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
    long previousSearcherVersion1 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();
    IndexSearcher previousSearcher1 =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .slm
            .acquire(previousSearcherVersion1);
    List<LeafReaderContext> previousLeaves1 = previousSearcher1.getIndexReader().leaves();
    replicaServer
        .getGlobalState()
        .getIndex("test_index")
        .getShard(0)
        .slm
        .release(previousSearcher1);

    primaryServer.addSimpleDocs("test_index", 6, 7, 8);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4, 5, 6, 7, 8);
    long previousSearcherVersion2 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();
    IndexSearcher previousSearcher2 =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .slm
            .acquire(previousSearcherVersion2);
    List<LeafReaderContext> previousLeaves2 = previousSearcher2.getIndexReader().leaves();
    replicaServer
        .getGlobalState()
        .getIndex("test_index")
        .getShard(0)
        .slm
        .release(previousSearcher2);

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
    long currentSearcherVersion1 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();
    IndexSearcher currentSearcher1 =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .slm
            .acquire(currentSearcherVersion1);
    List<LeafReaderContext> currentLeaves1 = currentSearcher1.getIndexReader().leaves();
    replicaServer.getGlobalState().getIndex("test_index").getShard(0).slm.release(currentSearcher1);

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    primaryServer.addSimpleDocs("test_index", 12);
    primaryServer.refresh("test_index");

    replicaServer.waitForReplication("test_index");
    long currentSearcherVersion2 =
        replicaServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .setQuery(Query.newBuilder().build())
                    .build())
            .getSearchState()
            .getSearcherVersion();
    IndexSearcher currentSearcher2 =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .slm
            .acquire(currentSearcherVersion2);
    List<LeafReaderContext> currentLeaves2 = currentSearcher2.getIndexReader().leaves();
    replicaServer.getGlobalState().getIndex("test_index").getShard(0).slm.release(currentSearcher2);

    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 9, 10, 11, 12);

    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion1, "test_index", 1, 2, 3, 4, 5);
    verifySimpleDocIdsForVersion(
        replicaServer, previousSearcherVersion2, "test_index", 1, 2, 3, 4, 5, 6, 7, 8);

    verifySegmentsClosed(previousLeaves1);
    verifySegmentsClosed(previousLeaves2);
    verifySegmentsClosed(currentLeaves1);
    verifySegmentsClosed(currentLeaves2);

    Thread.sleep(3000);
    replicaServer.getGlobalState().getIndex("test_index").getShard(0).slm.prune(new PruneByAge(1));

    verifySegmentsClosed(previousLeaves1, "_1");
    verifySegmentsClosed(previousLeaves2, "_1", "_2");
    verifySegmentsClosed(currentLeaves1);
    verifySegmentsClosed(currentLeaves2);
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

  private void verifySegmentsClosed(List<LeafReaderContext> leaves, String... segmentNames) {
    Set<String> expectedSegments = new HashSet<>(Arrays.asList(segmentNames));
    Set<String> closedSegments = new HashSet<>();
    for (LeafReaderContext context : leaves) {
      if (context.reader().getRefCount() == 0) {
        closedSegments.add(((SegmentReader) context.reader()).getSegmentName());
      }
    }
    assertEquals(expectedSegments, closedSegments);
  }
}
