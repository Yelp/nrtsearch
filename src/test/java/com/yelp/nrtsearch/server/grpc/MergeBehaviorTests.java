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
package com.yelp.nrtsearch.server.grpc;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static com.yelp.nrtsearch.server.grpc.LuceneServerTest.RETRIEVED_VALUES;
import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * These tests verify the behavior of segment merges on the searchers and on the segment files. For
 * old searchers to be pruned: 1. 60 seconds must pass after the searcher stopped being the live
 * searcher 2. if opened with a snapshot, the snapshot must be released
 *
 * <p>For old segment files (which have been merged into new segments) to be deleted: 1. Any
 * searchers referencing the segments must be pruned 2. If the segments were committed, a new commit
 * after the merge must be issued
 */
public class MergeBehaviorTests {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer grpcServer;

  private final int segmentsBeforeMerge = 2;
  private final int segmentsAfterMerge = 1;
  private final int numDocs = 4;

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    CollectorRegistry collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        luceneServerConfiguration.getPort(),
        null,
        Collections.emptyList());
  }

  @Test
  public void testForceMergeBehaviorWithoutCommitOrSnapshot()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = addFourDocsInTwoSegments();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, List.of(6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    testAddDocs.refresh();

    // Only the previous searcher present
    assertStats(segmentsAfterMerge, List.of(6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search both previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, List.of(8L, 6L));

    // Wait for 40 seconds
    sleep(40);

    // We still have the previous searcher
    assertStats(segmentsAfterMerge, List.of(8L, 6L));
    // No change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Wait for 22 more seconds
    sleep(22);

    // After waiting for 62 seconds total, the previous searcher is pruned (time when cleanup begins
    // is 60 seconds)
    assertStats(segmentsAfterMerge, List.of(8L));

    Set<String> segmentFilesAfterMergeAndSearcherPrune = getSegmentFiles();

    // initial empty index segments file
    assertTrue(segmentFilesBeforeMerge.remove("segments_1"));
    assertTrue(segmentFilesAfterMergeAndSearcherPrune.remove("segments_1"));

    // Some segment files deleted after searcher prune, no new segments added
    assertNotEquals(segmentFilesAfterMerge, segmentFilesAfterMergeAndSearcherPrune);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesAfterMergeAndSearcherPrune));
    // Pre-merge segments were deleted after searcher was pruned
    assertTrue(
        Sets.intersection(segmentFilesBeforeMerge, segmentFilesAfterMergeAndSearcherPrune)
            .isEmpty());
  }

  @Test
  public void testForceMergeBehaviorWithCommit() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = addFourDocsInTwoSegments();
    commit();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, List.of(6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    testAddDocs.refresh();

    // Only the previous searcher present
    assertStats(segmentsAfterMerge, List.of(6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search both previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, List.of(9L, 6L));

    // Wait for 40 seconds
    sleep(40);

    // We still have the previous searcher
    assertStats(segmentsAfterMerge, List.of(9L, 6L));
    // No change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Wait for 22 more seconds
    sleep(22);

    // After waiting for 62 seconds total, the previous searcher is pruned (time when cleanup begins
    // is 60 seconds)
    assertStats(segmentsAfterMerge, List.of(9L));

    // Previous segments not deleted yet
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    commit();
    sleep(2);

    Set<String> segmentFilesAfterMergeAndSearcherPrune = getSegmentFiles();

    // Remove the commit-specific files to compare the segments
    assertTrue(segmentFilesAfterMerge.remove("segments_2"));
    assertTrue(segmentFilesAfterMergeAndSearcherPrune.remove("segments_3"));

    // Some segment files deleted after searcher prune and commit, no new segments added
    assertNotEquals(segmentFilesAfterMerge, segmentFilesAfterMergeAndSearcherPrune);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesAfterMergeAndSearcherPrune));
    // Pre-merge segments were deleted after searcher was pruned and we issued a commit
    assertTrue(
        Sets.intersection(segmentFilesBeforeMerge, segmentFilesAfterMergeAndSearcherPrune)
            .isEmpty());
  }

  @Test
  public void testForceMergeBehaviorWithSnapshot() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = addFourDocsInTwoSegments();
    commit();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, List.of(6L));

    SnapshotId snapshotId = createSnapshot();

    // Another searcher opened after the snapshot was created
    assertStats(segmentsBeforeMerge, List.of(7L, 6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    testAddDocs.refresh();

    // Only the previous searchers present
    assertStats(segmentsAfterMerge, List.of(7L, 6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search all previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, List.of(9L, 7L, 6L));

    // Wait for 62 seconds
    sleep(62);

    // Searcher cleanup begins after 60 seconds but we still have the searcher opened with snapshot
    assertStats(segmentsAfterMerge, List.of(9L, 7L));
    // Also no change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Release snapshot and wait 2 seconds for the searcher to be pruned
    boolean success = releaseSnapshot(snapshotId);
    assertTrue(success);
    sleep(2);

    // After releasing the snapshot the snapshot searcher is pruned as the cleanup time has already
    // passed
    assertStats(segmentsAfterMerge, List.of(9L));

    Set<String> segmentFilesAfterMergeAndSnapshotRelease = getSegmentFiles();

    // Remove the snapshot files which are different
    assertTrue(segmentFilesAfterMerge.remove("snapshots_0"));
    assertTrue(segmentFilesAfterMergeAndSnapshotRelease.remove("snapshots_1"));

    // Even though the searchers were pruned, the segments are the same
    assertEquals(segmentFilesAfterMerge, segmentFilesAfterMergeAndSnapshotRelease);

    // Commit and wait
    commit();
    sleep(2);

    Set<String> segmentFilesAfterMergeReleaseAndCommit = getSegmentFiles();
    // Remove the commit and snapshot-specific files to compare the segments
    assertTrue(segmentFilesAfterMergeAndSnapshotRelease.remove("segments_2"));
    assertTrue(segmentFilesAfterMergeReleaseAndCommit.remove("segments_3"));
    assertTrue(segmentFilesAfterMergeReleaseAndCommit.remove("snapshots_1"));

    // Some segment files deleted after the commit, no new segments added
    assertNotEquals(segmentFilesAfterMerge, segmentFilesAfterMergeReleaseAndCommit);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesAfterMergeReleaseAndCommit));
    // Pre-merge segments were deleted after searcher was pruned due to snapshot release
    // and the commit was issued
    assertTrue(
        Sets.intersection(segmentFilesBeforeMerge, segmentFilesAfterMergeReleaseAndCommit)
            .isEmpty());
  }

  private GrpcServer.TestServer addFourDocsInTwoSegments()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // add 2 docs and create a segment
    testAddDocs.addDocuments();
    // add 2 more docs in a different segment
    testAddDocs.addDocuments();
    return testAddDocs;
  }

  private void commit() {
    CommitRequest commitRequest =
        CommitRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    grpcServer.getBlockingStub().commit(commitRequest);
  }

  /** Get and assert the stats. */
  private void assertStats(int numSegments, List<Long> searcherVersions) {
    StatsResponse stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(4, stats.getNumDocs());
    assertEquals(numSegments, stats.getCurrentSearcher().getNumSegments());
    assertEquals(searcherVersions.size(), stats.getSearchersList().size());

    for (int i = 0; i < searcherVersions.size(); i++) {
      assertEquals((long) searcherVersions.get(i), stats.getSearchers(i).getVersion());
    }
  }

  private Set<String> getSegmentFiles() throws IOException {
    return Files.list(getSegmentDirectory())
        .filter(path -> !path.getFileName().equals(Paths.get("write.lock")))
        .map(Path::getFileName)
        .map(Path::toString)
        .collect(Collectors.toSet());
  }

  private void doSearch() {
    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(numDocs + 1)
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());
    assertEquals(numDocs, searchResponse.getTotalHits().getValue());
  }

  private void doForceMerge() {
    ForceMergeResponse response =
        grpcServer
            .getBlockingStub()
            .forceMerge(
                ForceMergeRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setMaxNumSegments(segmentsAfterMerge)
                    .setDoWait(true)
                    .build());
    assertEquals(ForceMergeResponse.Status.FORCE_MERGE_COMPLETED, response.getStatus());
  }

  private Path getSegmentDirectory() throws IOException {
    return Paths.get(
        grpcServer.getIndexDir(),
        BackendGlobalState.getUniqueIndexName(
            grpcServer.getTestIndex(),
            grpcServer
                .getGlobalState()
                .getIndexStateManager(grpcServer.getTestIndex())
                .getIndexId()),
        "shard0",
        "index");
  }

  private void sleep(int seconds) throws InterruptedException {
    Thread.sleep(seconds * 1000);
  }

  private SnapshotId createSnapshot() {
    CreateSnapshotRequest request =
        CreateSnapshotRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setOpenSearcher(true)
            .build();
    return grpcServer.getBlockingStub().createSnapshot(request).getSnapshotId();
  }

  private boolean releaseSnapshot(SnapshotId snapshotId) {
    ReleaseSnapshotRequest request =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setSnapshotId(snapshotId)
            .build();
    return grpcServer.getBlockingStub().releaseSnapshot(request).getSuccess();
  }
}
