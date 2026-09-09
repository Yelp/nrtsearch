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

import static com.yelp.nrtsearch.server.grpc.NrtsearchServerTest.RETRIEVED_VALUES;
import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import com.yelp.nrtsearch.test_utils.TestResourceHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  private static final String TEST_INDEX = "test_index";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer server;

  private final int segmentsBeforeMerge = 2;
  private final int segmentsAfterMerge = 1;
  private final int numDocs = 4;

  @After
  public void tearDown() {
    TestServer.cleanupAll();
  }

  @Before
  public void setUp() throws Exception {
    server = TestServer.builder(folder).build();
    server.createIndex(TEST_INDEX);
    server
        .getClient()
        .getBlockingStub()
        .registerFields(
            TestResourceHelper.getFieldsFromResourceFile("/registerFieldsBasic.json").toBuilder()
                .setIndexName(TEST_INDEX)
                .build());
    server.startStandaloneIndex(TEST_INDEX, null);
  }

  @Test
  public void testForceMergeBehaviorWithoutCommitOrSnapshot() throws Exception {
    addFourDocsInTwoSegments();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, java.util.List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, java.util.List.of(6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    server.refresh(TEST_INDEX);

    // Only the previous searcher present
    assertStats(segmentsAfterMerge, java.util.List.of(6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search both previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, java.util.List.of(8L, 6L));

    // Wait for 40 seconds
    sleep(40);

    // We still have the previous searcher
    assertStats(segmentsAfterMerge, java.util.List.of(8L, 6L));
    // No change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Wait for 22 more seconds
    sleep(22);

    // After waiting for 62 seconds total, the previous searcher is pruned (time when cleanup begins
    // is 60 seconds)
    assertStats(segmentsAfterMerge, java.util.List.of(8L));

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
  public void testForceMergeBehaviorWithCommit() throws Exception {
    addFourDocsInTwoSegments();
    commit();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, java.util.List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, java.util.List.of(6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    server.refresh(TEST_INDEX);

    // Only the previous searcher present
    assertStats(segmentsAfterMerge, java.util.List.of(6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search both previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, java.util.List.of(9L, 6L));

    // Wait for 40 seconds
    sleep(40);

    // We still have the previous searcher
    assertStats(segmentsAfterMerge, java.util.List.of(9L, 6L));
    // No change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Wait for 22 more seconds
    sleep(22);

    // After waiting for 62 seconds total, the previous searcher is pruned (time when cleanup begins
    // is 60 seconds)
    assertStats(segmentsAfterMerge, java.util.List.of(9L));

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
  public void testForceMergeBehaviorWithSnapshot() throws Exception {
    addFourDocsInTwoSegments();
    commit();

    // Correct number of segments and no searchers in the response other than currentSearcher
    assertStats(segmentsBeforeMerge, java.util.List.of());

    doSearch();

    // Searcher present in the response after doing a search
    assertStats(segmentsBeforeMerge, java.util.List.of(6L));

    SnapshotId snapshotId = createSnapshot();

    // Another searcher opened after the snapshot was created
    assertStats(segmentsBeforeMerge, java.util.List.of(7L, 6L));

    Set<String> segmentFilesBeforeMerge = getSegmentFiles();

    doForceMerge();

    server.refresh(TEST_INDEX);

    // Only the previous searchers present
    assertStats(segmentsAfterMerge, java.util.List.of(7L, 6L));

    // After merge we have both pre-merge segments and the new merged segments
    Set<String> segmentFilesAfterMerge = getSegmentFiles();
    assertNotEquals(segmentFilesBeforeMerge, segmentFilesAfterMerge);
    assertTrue(segmentFilesAfterMerge.containsAll(segmentFilesBeforeMerge));

    doSearch();

    // After doing another search all previous and current searchers show up under searchers
    assertStats(segmentsAfterMerge, java.util.List.of(9L, 7L, 6L));

    // Wait for 62 seconds
    sleep(62);

    // Searcher cleanup begins after 60 seconds but we still have the searcher opened with snapshot
    assertStats(segmentsAfterMerge, java.util.List.of(9L, 7L));
    // Also no change in segment files since merge
    assertEquals(segmentFilesAfterMerge, getSegmentFiles());

    // Release snapshot and wait 2 seconds for the searcher to be pruned
    boolean success = releaseSnapshot(snapshotId);
    assertTrue(success);
    sleep(2);

    // After releasing the snapshot the snapshot searcher is pruned as the cleanup time has already
    // passed
    assertStats(segmentsAfterMerge, java.util.List.of(9L));

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

  private void addFourDocsInTwoSegments() throws Exception {
    // add 2 docs and create a segment
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    server.refresh(TEST_INDEX);
    // add 2 more docs in a different segment
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    server.refresh(TEST_INDEX);
  }

  private void commit() {
    server.commit(TEST_INDEX);
  }

  /** Get and assert the stats. */
  private void assertStats(int numSegments, java.util.List<Long> searcherVersions) {
    StatsResponse stats =
        server
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
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
        server
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(numDocs + 1)
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());
    assertEquals(numDocs, searchResponse.getTotalHits().getValue());
  }

  private void doForceMerge() {
    ForceMergeResponse response =
        server
            .getClient()
            .getBlockingStub()
            .forceMerge(
                ForceMergeRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setMaxNumSegments(segmentsAfterMerge)
                    .setDoWait(true)
                    .build());
    assertEquals(ForceMergeResponse.Status.FORCE_MERGE_COMPLETED, response.getStatus());
  }

  private Path getSegmentDirectory() throws IOException {
    return Paths.get(
        server.getGlobalState().getIndexDirBase().toString(),
        BackendGlobalState.getUniqueIndexName(
            TEST_INDEX,
            server.getGlobalState().getIndexStateManagerOrThrow(TEST_INDEX).getIndexId()),
        "shard0",
        "index");
  }

  private void sleep(int seconds) throws InterruptedException {
    Thread.sleep(seconds * 1000);
  }

  private SnapshotId createSnapshot() {
    CreateSnapshotRequest request =
        CreateSnapshotRequest.newBuilder().setIndexName(TEST_INDEX).setOpenSearcher(true).build();
    return server.getClient().getBlockingStub().createSnapshot(request).getSnapshotId();
  }

  private boolean releaseSnapshot(SnapshotId snapshotId) {
    ReleaseSnapshotRequest request =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setSnapshotId(snapshotId)
            .build();
    return server.getClient().getBlockingStub().releaseSnapshot(request).getSuccess();
  }
}
