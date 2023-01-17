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
package com.yelp.nrtsearch.server.luceneserver.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class BucketedTieredMergePolicyTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/VirtualShardsRegisterFields.json");
  }

  @Override
  public String getExtraConfig() {
    return "virtualSharding: true";
  }

  @Before
  public void clearIndex() throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;
    writer.deleteAll();
  }

  @Test
  public void testFindMerges() throws Exception {
    setLiveSettings(5, 10000, 100);
    addData(500);

    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    MergePolicy policy = shardState.writer.getConfig().getMergePolicy();
    assertTrue(policy instanceof BucketedTieredMergePolicy);

    waitForMerges(shardState);
    shardState.maybeRefreshBlocking();

    SearcherAndTaxonomy s = null;
    try {
      s = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).acquire();
      assertEquals(5, s.searcher.getSlices().length);
      for (LeafSlice slice : s.searcher.getSlices()) {
        assertTrue(slice.leaves.length < 100);
        int totalDocs = 0;
        for (LeafReaderContext context : slice.leaves) {
          totalDocs += context.reader().numDocs();
        }
        assertEquals(100, totalDocs);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
    verifyData(500);
  }

  @Test
  public void testFindForcedMerges() throws Exception {
    setLiveSettings(10, 10000, 100);
    addData(600);

    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    shardState.writer.forceMerge(1, true);
    waitForMerges(shardState);
    shardState.maybeRefreshBlocking();

    SearcherAndTaxonomy s = null;
    try {
      s = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).acquire();
      assertEquals(10, s.searcher.getSlices().length);
      for (LeafSlice slice : s.searcher.getSlices()) {
        assertEquals(1, slice.leaves.length);
        int totalDocs = 0;
        for (LeafReaderContext context : slice.leaves) {
          totalDocs += context.reader().numDocs();
        }
        assertEquals(60, totalDocs);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
    verifyData(600);

    // cut virtual shards in half and re-merge
    setLiveSettings(5, 10000, 100);
    shardState.writer.forceMerge(1, true);
    waitForMerges(shardState);
    shardState.maybeRefreshBlocking();

    s = null;
    try {
      s = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).acquire();
      assertEquals(5, s.searcher.getSlices().length);
      for (LeafSlice slice : s.searcher.getSlices()) {
        assertEquals(1, slice.leaves.length);
        int totalDocs = 0;
        for (LeafReaderContext context : slice.leaves) {
          totalDocs += context.reader().numDocs();
        }
        assertEquals(120, totalDocs);
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
    verifyData(600);
  }

  private void waitForMerges(ShardState shardState) throws IOException {
    shardState.writer.maybeMerge();
    int tries = 600;
    int count = 0;
    while (shardState.writer.hasPendingMerges()) {
      count++;
      if (count == tries) {
        throw new RuntimeException("Timed out waiting for merges");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    // for ConcurrentMergeScheduler this just forces a sync
    shardState.writer.getConfig().getMergeScheduler().close();
  }

  private void setLiveSettings(int virtualShards, int maxDocs, int maxSegments) throws IOException {
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder()
                .setVirtualShards(Int32Value.newBuilder().setValue(virtualShards).build())
                .setSliceMaxDocs(Int32Value.newBuilder().setValue(maxDocs).build())
                .setSliceMaxSegments(Int32Value.newBuilder().setValue(maxSegments).build())
                .build());
  }

  private void addData(int count) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;

    for (int i = 0; i < count; ++i) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(DEFAULT_TEST_INDEX)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i + 1))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i + 2))
                      .build())
              .build();
      addDocuments(Stream.of(request));
      writer.flush();
    }
    getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).maybeRefreshBlocking();
  }

  private void verifyData(int docs) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(docs + 100)
                    .setQuery(Query.newBuilder().build())
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_score")
                    .addRetrieveFields("int_field")
                    .build());
    assertEquals(docs, response.getHitsCount());
    Set<String> seenIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      seenIds.add(id);
      assertEquals(
          Integer.parseInt(id) + 1,
          hit.getFieldsOrThrow("int_score").getFieldValue(0).getIntValue());
      assertEquals(
          Integer.parseInt(id) + 2,
          hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    }
    assertEquals(docs, seenIds.size());
    for (int i = 0; i < docs; ++i) {
      assertTrue(seenIds.contains(String.valueOf(i)));
    }
  }

  @Test
  public void testBucketsForMerges() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(10, 10, 10, 10, 10));
    checkBucketing(2, infos, Arrays.asList(20, 30), Arrays.asList(2, 3));
    checkBucketing(3, infos, Arrays.asList(10, 20, 20), Arrays.asList(1, 2, 2));
    checkBucketing(4, infos, Arrays.asList(10, 10, 10, 20), Arrays.asList(1, 1, 1, 2));
    checkBucketing(5, infos, Arrays.asList(10, 10, 10, 10, 10), Arrays.asList(1, 1, 1, 1, 1));
    checkBucketing(6, infos, Arrays.asList(10, 10, 10, 10, 10), Arrays.asList(1, 1, 1, 1, 1));

    infos = getInfos(Arrays.asList(3, 5, 6, 7, 9));
    checkBucketing(2, infos, Arrays.asList(14, 16), Arrays.asList(2, 3));
    checkBucketing(3, infos, Arrays.asList(9, 10, 11), Arrays.asList(1, 2, 2));
    checkBucketing(4, infos, Arrays.asList(6, 7, 8, 9), Arrays.asList(1, 1, 2, 1));
  }

  @Test
  public void testBucketsByLiveDocs() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(3, 5, 7, 9, 10), Arrays.asList(0, 4, 3, 1, 5));
    checkBucketing(2, infos, Arrays.asList(10, 11), Arrays.asList(3, 2));
    checkBucketing(3, infos, Arrays.asList(6, 7, 8), Arrays.asList(2, 2, 1));
    checkBucketing(5, infos, Arrays.asList(1, 3, 4, 5, 8), Arrays.asList(1, 1, 1, 1, 1));
  }

  @Test
  public void testEmptyInfos() throws IOException {
    SegmentInfos infos = getInfos(Collections.emptyList());
    checkBucketing(2, infos, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testGetsAllMerges() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(9, 8, 7, 6, 5));
    List<MergeSpecification> bucketMerges = new ArrayList<>();

    bucketMerges.add(new MergeSpecification());

    MergeSpecification mergeSpecification = new MergeSpecification();
    mergeSpecification.add(new OneMerge(Arrays.asList(infos.info(2), infos.info(3))));
    bucketMerges.add(mergeSpecification);

    mergeSpecification = new MergeSpecification();
    mergeSpecification.add(new OneMerge(Arrays.asList(infos.info(1), infos.info(4))));
    bucketMerges.add(mergeSpecification);

    MergeSpecification aggregatedMerges = applyWithMerges(3, infos, bucketMerges);
    assertEquals(2, aggregatedMerges.merges.size());
    assertEquals(
        bucketMerges.get(1).merges.get(0).segments, aggregatedMerges.merges.get(0).segments);
    assertEquals(
        bucketMerges.get(2).merges.get(0).segments, aggregatedMerges.merges.get(1).segments);
  }

  @Test
  public void testNoMergesIsNull() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(9, 8, 7, 6, 5));
    List<MergeSpecification> bucketMerges = new ArrayList<>();
    bucketMerges.add(new MergeSpecification());
    bucketMerges.add(new MergeSpecification());
    bucketMerges.add(new MergeSpecification());
    MergeSpecification aggregatedMerges = applyWithMerges(3, infos, bucketMerges);
    assertNull(aggregatedMerges);
  }

  @Test
  public void testTracksPendingMerges() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(9, 8, 7, 6, 5));
    List<MergeSpecification> bucketMerges = new ArrayList<>();
    BucketedTieredMergePolicy btmp = new BucketedTieredMergePolicy(() -> 3);

    bucketMerges.add(new MergeSpecification());
    MergeSpecification mergeSpecification = new MergeSpecification();
    mergeSpecification.add(new OneMerge(Arrays.asList(infos.info(2), infos.info(3))));
    bucketMerges.add(mergeSpecification);
    bucketMerges.add(new MergeSpecification());

    MergeSpecification aggregatedMerges = applyWithMerges(btmp, 3, infos, bucketMerges);
    assertEquals(1, aggregatedMerges.merges.size());
    assertEquals(
        bucketMerges.get(1).merges.get(0).segments, aggregatedMerges.merges.get(0).segments);
    assertEquals(1, btmp.pendingMerges.size());
    assertEquals(Set.of("2", "3"), btmp.pendingMerges.get(0));

    bucketMerges = new ArrayList<>();
    bucketMerges.add(new MergeSpecification());
    bucketMerges.add(new MergeSpecification());
    mergeSpecification = new MergeSpecification();
    mergeSpecification.add(new OneMerge(Arrays.asList(infos.info(1), infos.info(4))));
    bucketMerges.add(mergeSpecification);

    aggregatedMerges = applyWithMerges(btmp, 3, infos, bucketMerges);
    assertEquals(1, aggregatedMerges.merges.size());
    assertEquals(
        bucketMerges.get(2).merges.get(0).segments, aggregatedMerges.merges.get(0).segments);
    assertEquals(2, btmp.pendingMerges.size());
    assertEquals(Set.of("1", "4"), btmp.pendingMerges.get(0));
    assertEquals(Set.of("2", "3"), btmp.pendingMerges.get(1));
  }

  @Test
  public void testRemovesCompletedMerges() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(10, 10, 10, 10, 10));
    BucketedTieredMergePolicy btmp = new BucketedTieredMergePolicy(() -> 3);
    btmp.pendingMerges.add(Set.of("1", "3"));
    btmp.pendingMerges.add(Set.of("0", "5"));
    btmp.pendingMerges.add(Set.of("2", "4"));
    btmp.pendingMerges.add(Set.of("6", "7"));

    checkBucketing(btmp, 3, infos, Arrays.asList(10, 20, 20), Arrays.asList(1, 2, 2));
    assertEquals(2, btmp.pendingMerges.size());
    assertEquals(Set.of("1", "3"), btmp.pendingMerges.get(0));
    assertEquals(Set.of("2", "4"), btmp.pendingMerges.get(1));
  }

  @Test
  public void testGroupsMergingSegments() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(3, 9, 1, 8, 1));
    BucketedTieredMergePolicy btmp = new BucketedTieredMergePolicy(() -> 3);
    btmp.pendingMerges.add(Set.of("0", "2", "4"));

    checkBucketing(btmp, 3, infos, Arrays.asList(5, 8, 9), Arrays.asList(3, 1, 1));
  }

  @Test
  public void testRemovesShadowedMerges() throws IOException {
    SegmentInfos infos = getInfos(Arrays.asList(3, 9, 1, 8, 1));
    BucketedTieredMergePolicy btmp = new BucketedTieredMergePolicy(() -> 3);
    btmp.pendingMerges.add(Set.of("0", "1", "2"));
    btmp.pendingMerges.add(Set.of("2", "3", "4"));

    checkBucketing(btmp, 3, infos, Arrays.asList(1, 8, 13), Arrays.asList(1, 1, 3));
    assertEquals(1, btmp.pendingMerges.size());
    assertEquals(Set.of("0", "1", "2"), btmp.pendingMerges.get(0));
  }

  private MergeSpecification applyWithMerges(
      int buckets, SegmentInfos infos, List<MergeSpecification> bucketMerges) throws IOException {
    BucketedTieredMergePolicy btmp = new BucketedTieredMergePolicy(() -> buckets);
    return applyWithMerges(btmp, buckets, infos, bucketMerges);
  }

  private MergeSpecification applyWithMerges(
      BucketedTieredMergePolicy btmp,
      int buckets,
      SegmentInfos infos,
      List<MergeSpecification> bucketMerges)
      throws IOException {
    AtomicInteger index = new AtomicInteger();
    return btmp.findForSegmentInfos(
        buckets, infos, (si) -> bucketMerges.get(index.getAndIncrement()));
  }

  private void checkBucketing(
      int buckets, SegmentInfos infos, List<Integer> bucketSizes, List<Integer> segmentCounts)
      throws IOException {
    checkBucketing(
        new BucketedTieredMergePolicy(() -> buckets), buckets, infos, bucketSizes, segmentCounts);
  }

  private void checkBucketing(
      BucketedTieredMergePolicy btmp,
      int buckets,
      SegmentInfos infos,
      List<Integer> bucketSizes,
      List<Integer> segmentCounts)
      throws IOException {
    assertEquals(bucketSizes.size(), segmentCounts.size());

    AtomicInteger index = new AtomicInteger();
    Set<String> seenNames = new HashSet<>();

    btmp.findForSegmentInfos(
        buckets,
        infos,
        (si) -> {
          int currentIndex = index.getAndIncrement();
          assertEquals(segmentCounts.get(currentIndex), Integer.valueOf(si.size()));
          int totalSize = 0;
          for (SegmentCommitInfo sci : si) {
            seenNames.add(sci.info.name);
            totalSize += (sci.info.maxDoc() - sci.getDelCount());
          }
          assertEquals(bucketSizes.get(currentIndex), Integer.valueOf(totalSize));
          return new MergeSpecification();
        });

    assertEquals(bucketSizes.size(), index.get());

    assertEquals(infos.size(), seenNames.size());
    for (int i = 0; i < infos.size(); ++i) {
      assertTrue(seenNames.contains(String.valueOf(i)));
    }
  }

  private SegmentInfos getInfos(List<Integer> sizes) {
    return getInfos(sizes, Collections.nCopies(sizes.size(), 0));
  }

  private SegmentInfos getInfos(List<Integer> sizes, List<Integer> deletions) {
    SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
    Directory mockDir = mock(Directory.class);
    int id = 0;
    for (int i = 0; i < sizes.size(); ++i) {
      SegmentInfo si =
          new SegmentInfo(
              mockDir,
              Version.LATEST,
              Version.LATEST,
              String.valueOf(id),
              sizes.get(i),
              false,
              null,
              Collections.emptyMap(),
              new byte[StringHelper.ID_LENGTH],
              Collections.emptyMap(),
              null);
      SegmentCommitInfo sci = new SegmentCommitInfo(si, deletions.get(i), 0, 1, 1, 1);
      infos.add(sci);
      id++;
    }
    return infos;
  }
}
