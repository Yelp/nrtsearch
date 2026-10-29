/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.search.MyIndexSearcher.SliceStrategy;
import com.yelp.nrtsearch.server.search.MyIndexSearcher.SlicingParams;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher.LeafReaderContextPartition;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MyIndexSearcherPartitionSlicesTest {
  private static ExecutorService executor;

  @BeforeClass
  public static void setUpClass() {
    executor = Executors.newFixedThreadPool(4);
  }

  @AfterClass
  public static void tearDownClass() {
    executor.shutdown();
  }

  /** Index with one segment per entry of segmentDocs, deleting every deleteEvery-th doc if > 0. */
  private static DirectoryReader buildIndex(Directory dir, int[] segmentDocs, int deleteEvery)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      int id = 0;
      for (int numDocs : segmentDocs) {
        for (int i = 0; i < numDocs; ++i) {
          Document doc = new Document();
          doc.add(new StringField("id", String.valueOf(id), Field.Store.NO));
          doc.add(new StringField("mod", String.valueOf(id % 7), Field.Store.NO));
          doc.add(new IntPoint("value", id));
          writer.addDocument(doc);
          id++;
        }
        writer.flush();
      }
      if (deleteEvery > 0) {
        for (int i = 0; i < id; i += deleteEvery) {
          writer.deleteDocuments(new Term("id", String.valueOf(i)));
        }
      }
      writer.commit();
    }
    return DirectoryReader.open(dir);
  }

  /** Index of nested doc blocks, one segment per entry, each block has blockChildren children. */
  private static DirectoryReader buildNestedIndex(
      Directory dir, int[] segmentBlocks, int blockChildren) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      int id = 0;
      for (int numBlocks : segmentBlocks) {
        for (int b = 0; b < numBlocks; ++b) {
          List<Document> block = new ArrayList<>();
          for (int c = 0; c < blockChildren; ++c) {
            Document child = new Document();
            child.add(new StringField("type", "child", Field.Store.NO));
            block.add(child);
          }
          Document parent = new Document();
          parent.add(new StringField("type", "parent", Field.Store.NO));
          parent.add(new StringField("id", String.valueOf(id++), Field.Store.NO));
          block.add(parent);
          writer.addDocuments(block);
        }
        writer.flush();
      }
      writer.commit();
    }
    return DirectoryReader.open(dir);
  }

  /**
   * Verify that slices cover every doc in every segment exactly once, and that no slice contains
   * multiple partitions of the same segment.
   */
  private static void assertValidSlices(List<LeafReaderContext> leaves, LeafSlice[] slices) {
    Map<LeafReaderContext, List<int[]>> ranges = new HashMap<>();
    for (LeafSlice slice : slices) {
      Set<LeafReaderContext> sliceSegments = new HashSet<>();
      for (LeafReaderContextPartition partition : slice.partitions) {
        assertTrue("segment in slice multiple times", sliceSegments.add(partition.ctx));
        int maxDoc = partition.ctx.reader().maxDoc();
        int to = Math.min(partition.maxDocId, maxDoc);
        assertTrue(partition.minDocId < to);
        ranges
            .computeIfAbsent(partition.ctx, k -> new ArrayList<>())
            .add(new int[] {partition.minDocId, to});
      }
    }
    for (LeafReaderContext leaf : leaves) {
      if (leaf.reader().maxDoc() == 0) {
        continue;
      }
      List<int[]> leafRanges = ranges.get(leaf);
      assertTrue("segment not in any slice", leafRanges != null);
      leafRanges.sort((a, b) -> Integer.compare(a[0], b[0]));
      int next = 0;
      for (int[] range : leafRanges) {
        assertEquals("gap or overlap in segment partitions", next, range[0]);
        next = range[1];
      }
      assertEquals(leaf.reader().maxDoc(), next);
    }
  }

  private static long liveDocs(LeafReaderContextPartition partition) throws IOException {
    MyIndexSearcher.SegmentDocs docs = new MyIndexSearcher.SegmentDocs(partition.ctx, null);
    return docs.liveDocs(partition.minDocId, Math.min(partition.maxDocId, docs.maxDoc));
  }

  private static long[] sliceLiveDocs(LeafSlice[] slices) throws IOException {
    long[] docs = new long[slices.length];
    for (int i = 0; i < slices.length; ++i) {
      for (LeafReaderContextPartition partition : slices[i].partitions) {
        docs[i] += liveDocs(partition);
      }
    }
    return docs;
  }

  private static double imbalance(long[] sliceDocs) {
    long total = 0;
    long max = 0;
    for (long d : sliceDocs) {
      total += d;
      max = Math.max(max, d);
    }
    return max / ((double) total / sliceDocs.length);
  }

  private static int numPartitions(LeafSlice[] slices) {
    int n = 0;
    for (LeafSlice slice : slices) {
      n += slice.partitions.length;
    }
    return n;
  }

  @Test
  public void testBalancedCut_oneLargeSegment() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {5000, 1100, 1000, 600, 300}, 0)) {
      LeafSlice[] slices = MyIndexSearcher.balancedCutSlices(reader.leaves(), 4, 100, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      assertEquals(4, slices.length);
      assertTrue(imbalance(sliceLiveDocs(slices)) <= 1.1);
      // at most targetSlices - 1 extra partitions
      assertTrue(numPartitions(slices) <= reader.leaves().size() + 3);
    }
  }

  @Test
  public void testBalancedCut_snapsToBoundary() throws IOException {
    // equal segments that align exactly with slice boundaries should never be split
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {1000, 1000, 1000, 1000}, 0)) {
      LeafSlice[] slices = MyIndexSearcher.balancedCutSlices(reader.leaves(), 4, 100, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      assertEquals(4, slices.length);
      for (LeafSlice slice : slices) {
        assertEquals(1, slice.partitions.length);
        assertEquals(0, slice.partitions[0].minDocId);
      }
    }
  }

  @Test
  public void testBalancedCut_manySmallSegments() throws IOException {
    int[] segments = new int[40];
    java.util.Arrays.fill(segments, 50);
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, segments, 0)) {
      LeafSlice[] slices = MyIndexSearcher.balancedCutSlices(reader.leaves(), 8, 100, 0.2, null);
      assertValidSlices(reader.leaves(), slices);
      assertEquals(8, slices.length);
      // segments are small relative to the slice size, so none are split
      assertEquals(40, numPartitions(slices));
    }
  }

  @Test
  public void testBalancedCut_withDeletes() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {8000, 500, 300}, 3)) {
      LeafSlice[] slices = MyIndexSearcher.balancedCutSlices(reader.leaves(), 4, 100, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      long[] docs = sliceLiveDocs(slices);
      long total = 0;
      for (long d : docs) {
        total += d;
      }
      assertEquals(reader.numDocs(), total);
      assertTrue(imbalance(docs) <= 1.1);
    }
  }

  @Test
  public void testBalancedCut_minPartitionDocsLimitsSlices() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {1000}, 0)) {
      LeafSlice[] slices = MyIndexSearcher.balancedCutSlices(reader.leaves(), 16, 400, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      // 1000 docs only supports 2 partitions of at least 400 docs
      assertEquals(2, slices.length);
      for (long docs : sliceLiveDocs(slices)) {
        assertTrue(docs >= 400);
      }
    }
  }

  @Test
  public void testSplitOutliers_oneLargeSegment() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {5000, 1100, 1000, 600, 300}, 0)) {
      LeafSlice[] slices = MyIndexSearcher.splitOutlierSlices(reader.leaves(), 4, 100, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      assertEquals(4, slices.length);
      assertTrue(imbalance(sliceLiveDocs(slices)) <= 1.25);
      // only the large segment is split
      MyIndexSearcher.SliceStats stats = MyIndexSearcher.getSliceStats(slices);
      assertEquals(1, stats.numSplitSegments());
    }
  }

  @Test
  public void testSplitOutliers_noOutliers() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {500, 400, 300, 200, 100}, 0)) {
      LeafSlice[] slices = MyIndexSearcher.splitOutlierSlices(reader.leaves(), 3, 50, 0.1, null);
      assertValidSlices(reader.leaves(), slices);
      assertEquals(0, MyIndexSearcher.getSliceStats(slices).numSplitSegments());
    }
  }

  @Test
  public void testNestedBlocksNotSplit() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildNestedIndex(dir, new int[] {1000, 50}, 4)) {
      BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
      for (LeafSlice[] slices :
          List.of(
              MyIndexSearcher.balancedCutSlices(reader.leaves(), 7, 10, 0.05, parents),
              MyIndexSearcher.splitOutlierSlices(reader.leaves(), 7, 10, 0.05, parents))) {
        assertValidSlices(reader.leaves(), slices);
        assertTrue(MyIndexSearcher.getSliceStats(slices).numSplitSegments() > 0);
        for (LeafSlice slice : slices) {
          for (LeafReaderContextPartition partition : slice.partitions) {
            int maxDoc = partition.ctx.reader().maxDoc();
            int to = Math.min(partition.maxDocId, maxDoc);
            BitSet parentBits = parents.getBitSet(partition.ctx);
            // every partition ends right after a parent document
            assertTrue(parentBits.get(to - 1));
          }
        }
      }
    }
  }

  @Test
  public void testEmptyIndex() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {}, 0)) {
      assertEquals(0, MyIndexSearcher.balancedCutSlices(reader.leaves(), 4, 1, 0.1, null).length);
      assertEquals(0, MyIndexSearcher.splitOutlierSlices(reader.leaves(), 4, 1, 0.1, null).length);
    }
  }

  private static SlicingParams params(SliceStrategy strategy) {
    return new SlicingParams(250_000, 5, 1, strategy, 4, 100, 0.1, null);
  }

  @Test
  public void testSearchResultsMatchSegmentSlicing() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {5000, 1100, 1000, 600, 300}, 5)) {
      MyIndexSearcher segmentSearcher =
          MyIndexSearcher.create(reader, executor, params(SliceStrategy.SEGMENT));
      List<Query> queries =
          List.of(
              new MatchAllDocsQuery(),
              new TermQuery(new Term("mod", "3")),
              IntPoint.newRangeQuery("value", 1000, 6500),
              IntPoint.newSetQuery("value", 1, 10, 4999, 5000, 5001, 7999));
      for (SliceStrategy strategy :
          List.of(SliceStrategy.BALANCED_CUT, SliceStrategy.SPLIT_OUTLIERS)) {
        MyIndexSearcher searcher = MyIndexSearcher.create(reader, executor, params(strategy));
        assertTrue(MyIndexSearcher.getSliceStats(searcher.getSlices()).numSplitSegments() > 0);
        for (Query query : queries) {
          int expectedCount =
              segmentSearcher.search(
                  query, new TotalHitCountCollectorManager(segmentSearcher.getSlices()));
          int count =
              searcher.search(query, new TotalHitCountCollectorManager(searcher.getSlices()));
          assertEquals(query.toString(), expectedCount, count);
          assertEquals(query.toString(), expectedCount, searcher.count(query));

          TopDocs expected = segmentSearcher.search(query, 50);
          TopDocs actual = searcher.search(query, 50);
          // past the total hits threshold, the hit count is a lower bound that depends on slicing
          if (expected.totalHits.relation() == TotalHits.Relation.EQUAL_TO
              && actual.totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
            assertEquals(expected.totalHits, actual.totalHits);
          }
          assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
          for (int i = 0; i < expected.scoreDocs.length; ++i) {
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
          }
        }
      }
    }
  }

  @Test
  public void testHitCountCollectorManagerWithPartitions() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {5000, 100}, 0)) {
      MyIndexSearcher searcher =
          MyIndexSearcher.create(reader, executor, params(SliceStrategy.BALANCED_CUT));
      assertTrue(MyIndexSearcher.getSliceStats(searcher.getSlices()).numSplitSegments() > 0);
      // match all can use Weight.count for the whole segment, which must only be counted once
      TopDocs topDocs =
          searcher.search(
              new MatchAllDocsQuery(),
              new com.yelp.nrtsearch.server.search.collectors.HitCountCollector
                  .HitCountCollectorManager(searcher.getSlices()));
      assertEquals(5100, topDocs.totalHits.value());
    }
  }

  @Test
  public void testSegmentSearcher() throws IOException {
    try (Directory dir = new ByteBuffersDirectory();
        DirectoryReader reader = buildIndex(dir, new int[] {5000, 100}, 0)) {
      MyIndexSearcher segmentOnly =
          MyIndexSearcher.create(reader, executor, params(SliceStrategy.SEGMENT));
      assertSame(segmentOnly, segmentOnly.getSegmentSearcher());

      MyIndexSearcher searcher =
          MyIndexSearcher.create(reader, executor, params(SliceStrategy.BALANCED_CUT));
      MyIndexSearcher wholeSegments = searcher.getSegmentSearcher();
      assertSame(wholeSegments, searcher.getSegmentSearcher());
      assertSame(searcher.getIndexReader(), wholeSegments.getIndexReader());
      assertSame(searcher.getSimilarity(), wholeSegments.getSimilarity());
      assertEquals(0, MyIndexSearcher.getSliceStats(wholeSegments.getSlices()).numSplitSegments());
      assertFalse(wholeSegments.getSlicingParams().partitionsSegments());
    }
  }
}
