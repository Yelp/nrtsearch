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
package com.yelp.nrtsearch.server.search;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Custom IndexSearcher that allows for custom slicing of the index into multiple segments for
 * parallel search.
 *
 * <p>Some {@link SliceStrategy} values divide large segments into multiple doc id range partitions,
 * so that a single large segment can be searched in parallel. Not all queries support segment
 * partitions, so {@link #getSegmentSearcher()} provides a view of the same index that only uses
 * whole segment slices.
 */
public class MyIndexSearcher extends IndexSearcher {

  /** Strategy used to divide the index into parallel search slices. */
  public enum SliceStrategy {
    /** Group whole segments, limited by max docs and max segments per slice. */
    SEGMENT,
    /**
     * Lay out all segments end to end and cut into slices with equal live docs. A cut close to a
     * segment boundary moves to the boundary, otherwise the segment is split at the cut.
     */
    BALANCED_CUT,
    /**
     * Split each segment that is larger than the target slice size into equal partitions, then pack
     * all segments and partitions into the target number of slices, largest first.
     */
    SPLIT_OUTLIERS
  }

  /**
   * Parameters for dividing the index into parallel search slices.
   *
   * @param sliceMaxDocs max docs per slice, used by SEGMENT strategy
   * @param sliceMaxSegments max segments per slice, used by SEGMENT strategy
   * @param virtualShards number of virtual shards, used by SEGMENT strategy
   * @param sliceStrategy strategy to divide the index into slices
   * @param sliceTarget target number of slices for partitioning strategies
   * @param sliceMinPartitionDocs minimum live docs in a partial segment partition
   * @param sliceSnapTolerance fraction of the target slice size a cut may move to land on a segment
   *     boundary
   * @param parentBitSetProducer producer for nested document parents, cuts are only made after a
   *     parent document so that a nested document block is never split, or null if index has no
   *     nested documents
   */
  public record SlicingParams(
      int sliceMaxDocs,
      int sliceMaxSegments,
      int virtualShards,
      SliceStrategy sliceStrategy,
      int sliceTarget,
      int sliceMinPartitionDocs,
      double sliceSnapTolerance,
      BitSetProducer parentBitSetProducer) {

    /** Create params for the SEGMENT slice strategy. */
    public SlicingParams(int sliceMaxDocs, int sliceMaxSegments, int virtualShards) {
      this(sliceMaxDocs, sliceMaxSegments, virtualShards, SliceStrategy.SEGMENT, 1, 1, 0.0, null);
    }

    /** If this strategy may divide a segment into multiple partitions. */
    public boolean partitionsSegments() {
      return sliceStrategy != SliceStrategy.SEGMENT;
    }
  }

  private final SlicingParams slicingParams;
  private final Executor executor;
  private volatile MyIndexSearcher segmentSearcher;
  private volatile long sliceComputeNanos = 0;

  /**
   * Create a new MyIndexSearcher.
   *
   * @param reader index reader
   * @param executor parallel search task executor
   * @param slicingParams slicing parameters
   * @return MyIndexSearcher
   */
  public static MyIndexSearcher create(
      IndexReader reader, Executor executor, SlicingParams slicingParams) {
    return new MyIndexSearcher(reader, executor, slicingParams);
  }

  /**
   * Constructor.
   *
   * @param reader index reader
   * @param executor parallel search task executor
   * @param slicingParams slicing parameters
   */
  protected MyIndexSearcher(IndexReader reader, Executor executor, SlicingParams slicingParams) {
    super(reader, executor);
    if (slicingParams == null) {
      throw new IllegalArgumentException("Slicing params not set");
    }
    // IndexSearcher computes slices lazily on first use, so the params must be held by the
    // instance instead of being passed in a static context during construction
    this.slicingParams = slicingParams;
    this.executor = executor;
  }

  @VisibleForTesting
  SlicingParams getSlicingParams() {
    return slicingParams;
  }

  /** Get the time taken to compute the slices for this searcher, or 0 if not yet computed. */
  public long getSliceComputeNanos() {
    return sliceComputeNanos;
  }

  /**
   * Get statistics about how this searcher divides the index into slices. This computes the slices
   * if they have not been already.
   *
   * @return slice statistics
   */
  public SliceStats getSliceStats() {
    try {
      return getSliceStats(getSlices());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Get a searcher for the same index that only uses whole segment slices. This should be used for
   * queries that do not support searching a doc id range of a segment, such as drill sideways and
   * completion queries. If this searcher does not partition segments, it is returned directly.
   *
   * <p>The returned searcher has the same similarity, query cache, caching policy and timeout as
   * this searcher at the time of the first call.
   *
   * @return searcher using only whole segment slices
   */
  public MyIndexSearcher getSegmentSearcher() {
    if (!slicingParams.partitionsSegments()) {
      return this;
    }
    MyIndexSearcher searcher = segmentSearcher;
    if (searcher == null) {
      synchronized (this) {
        searcher = segmentSearcher;
        if (searcher == null) {
          searcher =
              new MyIndexSearcher(
                  getIndexReader(),
                  executor,
                  new SlicingParams(
                      slicingParams.sliceMaxDocs(),
                      slicingParams.sliceMaxSegments(),
                      slicingParams.virtualShards()));
          searcher.setSimilarity(getSimilarity());
          searcher.setQueryCache(getQueryCache());
          searcher.setQueryCachingPolicy(getQueryCachingPolicy());
          searcher.setTimeout(getTimeout());
          segmentSearcher = searcher;
        }
      }
    }
    return searcher;
  }

  /** * start segment to thread mapping * */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    long startNanos = System.nanoTime();
    try {
      return computeSlices(leaves);
    } finally {
      sliceComputeNanos = System.nanoTime() - startNanos;
    }
  }

  private LeafSlice[] computeSlices(List<LeafReaderContext> leaves) {
    try {
      return switch (slicingParams.sliceStrategy()) {
        case SEGMENT -> {
          if (slicingParams.virtualShards() > 1) {
            yield slicesForShards(
                leaves,
                slicingParams.virtualShards(),
                slicingParams.sliceMaxDocs(),
                slicingParams.sliceMaxSegments());
          } else {
            yield slices(leaves, slicingParams.sliceMaxDocs(), slicingParams.sliceMaxSegments());
          }
        }
        case BALANCED_CUT ->
            balancedCutSlices(
                leaves,
                slicingParams.sliceTarget(),
                slicingParams.sliceMinPartitionDocs(),
                slicingParams.sliceSnapTolerance(),
                slicingParams.parentBitSetProducer());
        case SPLIT_OUTLIERS ->
            splitOutlierSlices(
                leaves,
                slicingParams.sliceTarget(),
                slicingParams.sliceMinPartitionDocs(),
                slicingParams.sliceSnapTolerance(),
                slicingParams.parentBitSetProducer());
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Class to hold the segments in a virtual shard and the total live doc count. */
  private static class VirtualShardLeaves {
    List<LeafReaderContext> leaves = new ArrayList<>();
    long numDocs = 0;

    void add(LeafReaderContext leaf) {
      leaves.add(leaf);
      numDocs += leaf.reader().numDocs();
    }
  }

  /** Class to hold an index slice and the total slice live doc count. */
  private static class SliceAndSize {
    LeafSlice slice;
    long numDocs = 0;

    SliceAndSize(LeafSlice slice) {
      this(slice, slice.getMaxDocs());
    }

    SliceAndSize(LeafSlice slice, long numDocs) {
      this.slice = slice;
      this.numDocs = numDocs;
    }
  }

  private static LeafSlice[] slicesForShards(
      List<LeafReaderContext> leaves, int virtualShards, int sliceMaxDocs, int sliceMaxSegments) {
    if (leaves.isEmpty()) {
      return new LeafSlice[0];
    }
    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
    // Sort by number of live documents, descending:
    sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().numDocs())));

    // Create container for each virtual shard, add them to a min heap by number of live documents
    PriorityQueue<VirtualShardLeaves> shardQueue =
        new PriorityQueue<>(sortedLeaves.size(), Comparator.comparingLong(sl -> sl.numDocs));
    for (int i = 0; i < virtualShards; ++i) {
      shardQueue.add(new VirtualShardLeaves());
    }

    // Add each segment in sequence to the virtual shard with the least live documents
    for (LeafReaderContext leaf : sortedLeaves) {
      VirtualShardLeaves shardLeaves = shardQueue.poll();
      shardLeaves.add(leaf);
      shardQueue.add(shardLeaves);
    }

    // compute the parallel search slices for each shard independently and combine them
    PriorityQueue<SliceAndSize> sortedSlices =
        new PriorityQueue<>(Collections.reverseOrder(Comparator.comparingLong(ss -> ss.numDocs)));
    while (!shardQueue.isEmpty()) {
      VirtualShardLeaves shardLeaves = shardQueue.poll();
      if (!shardLeaves.leaves.isEmpty()) {
        LeafSlice[] shardSlices = slices(shardLeaves.leaves, sliceMaxDocs, sliceMaxSegments);
        for (LeafSlice leafSlice : shardSlices) {
          sortedSlices.add(new SliceAndSize(leafSlice));
        }
      }
    }

    // order slices largest to smallest
    LeafSlice[] slices = new LeafSlice[sortedSlices.size()];
    for (int i = 0; i < slices.length; ++i) {
      slices[i] = sortedSlices.poll().slice;
    }
    return slices;
  }

  /** Static method to segregate LeafReaderContexts amongst multiple slices */
  public static LeafSlice[] slices(
      List<LeafReaderContext> leaves, int maxDocsPerSlice, int maxSegmentsPerSlice) {
    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

    // Sort by maxDoc, descending:
    Collections.sort(
        sortedLeaves, Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    final List<List<LeafReaderContextPartition>> groupedLeaves = new ArrayList<>();
    long docSum = 0;
    List<LeafReaderContextPartition> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
      if (ctx.reader().maxDoc() > maxDocsPerSlice) {
        assert group == null;
        groupedLeaves.add(
            Collections.singletonList(LeafReaderContextPartition.createForEntireSegment(ctx)));
      } else {
        if (group == null) {
          group = new ArrayList<>();
          group.add(LeafReaderContextPartition.createForEntireSegment(ctx));

          groupedLeaves.add(group);
        } else {
          group.add(LeafReaderContextPartition.createForEntireSegment(ctx));
        }

        docSum += ctx.reader().maxDoc();
        if (group.size() >= maxSegmentsPerSlice || docSum > maxDocsPerSlice) {
          group = null;
          docSum = 0;
        }
      }
    }

    LeafSlice[] slices = new LeafSlice[groupedLeaves.size()];
    int upto = 0;
    for (List<LeafReaderContextPartition> currentLeaf : groupedLeaves) {
      // LeafSlice constructor has changed in 9.x. This allows to use old constructor.
      Collections.sort(currentLeaf, Comparator.comparingInt(l -> l.ctx.docBase));
      slices[upto] = new LeafSlice(currentLeaf);
      ++upto;
    }

    return slices;
  }

  /**
   * Live doc counts and valid cut points for a single segment. A cut at doc id {@code d} means
   * partitions {@code [.., d)} and {@code [d, ..)}.
   */
  @VisibleForTesting
  static class SegmentDocs {
    final LeafReaderContext ctx;
    final int maxDoc;
    final int numDocs;
    private final BitSetProducer parentBitSetProducer;
    // Only loaded if this segment needs to be split. Null if all docs are live, or the index has
    // no nested documents.
    private boolean loaded = false;
    private FixedBitSet liveDocs;
    private BitSet parents;

    SegmentDocs(LeafReaderContext ctx, BitSetProducer parentBitSetProducer) {
      this.ctx = ctx;
      this.maxDoc = ctx.reader().maxDoc();
      this.numDocs = ctx.reader().numDocs();
      this.parentBitSetProducer = parentBitSetProducer;
    }

    private void load() throws IOException {
      if (loaded) {
        return;
      }
      Bits readerLiveDocs = ctx.reader().getLiveDocs();
      if (readerLiveDocs != null && numDocs != maxDoc) {
        liveDocs = new FixedBitSet(maxDoc);
        liveDocs.set(0, maxDoc);
        readerLiveDocs.applyMask(liveDocs, 0);
      }
      if (parentBitSetProducer != null) {
        parents = parentBitSetProducer.getBitSet(ctx);
      }
      loaded = true;
    }

    /** Number of live docs in the range {@code [from, to)}. */
    int liveDocs(int from, int to) throws IOException {
      if (from >= to) {
        return 0;
      }
      if (from == 0 && to >= maxDoc) {
        return numDocs;
      }
      load();
      if (liveDocs == null) {
        return to - from;
      }
      return liveDocs.cardinality(from, to);
    }

    /**
     * Find the doc id to cut this segment at, so that the range {@code [from, cut)} contains about
     * {@code targetLiveDocs} live documents. The cut is adjusted forward so that it does not split
     * a nested document block.
     *
     * @return cut doc id in {@code (from, maxDoc]}
     */
    int findCut(int from, long targetLiveDocs) throws IOException {
      load();
      int cut;
      if (targetLiveDocs <= 0) {
        cut = from + 1;
      } else if (liveDocs == null) {
        cut = (int) Math.min(maxDoc, from + targetLiveDocs);
      } else {
        // binary search for the smallest cut with at least targetLiveDocs live docs before it
        int lo = from + 1;
        int hi = maxDoc;
        while (lo < hi) {
          int mid = (lo + hi) >>> 1;
          if (liveDocs(from, mid) >= targetLiveDocs) {
            hi = mid;
          } else {
            lo = mid + 1;
          }
        }
        cut = lo;
      }
      return alignToBlock(cut);
    }

    /**
     * Align a cut so that it is right after a parent document. Child documents are indexed before
     * their parent, so a valid cut is one where doc {@code cut - 1} is a parent.
     */
    int alignToBlock(int cut) {
      if (parents == null || cut >= maxDoc) {
        return Math.min(cut, maxDoc);
      }
      if (cut > 0 && parents.get(cut - 1)) {
        return cut;
      }
      int nextParent = parents.nextSetBit(cut);
      return nextParent == DocIdSetIterator.NO_MORE_DOCS ? maxDoc : nextParent + 1;
    }

    LeafReaderContextPartition partition(int from, int to) {
      if (from == 0 && to >= maxDoc) {
        return LeafReaderContextPartition.createForEntireSegment(ctx);
      }
      return LeafReaderContextPartition.createFromAndTo(ctx, from, to);
    }
  }

  private static List<SegmentDocs> segmentDocs(
      List<LeafReaderContext> leaves, BitSetProducer parentBitSetProducer) {
    List<SegmentDocs> segments = new ArrayList<>(leaves.size());
    for (LeafReaderContext ctx : leaves) {
      if (ctx.reader().maxDoc() > 0) {
        segments.add(new SegmentDocs(ctx, parentBitSetProducer));
      }
    }
    return segments;
  }

  /**
   * Divide the index into slices by laying out all segments end to end in doc id order, and cutting
   * into {@code targetSlices} pieces with an equal number of live documents. If a cut is within
   * {@code snapTolerance * targetSliceDocs} live docs of a segment boundary, it moves to the
   * boundary. Otherwise, the segment is split at the cut, as long as both sides have at least
   * {@code minPartitionDocs} live docs.
   *
   * <p>Since cuts are made in order, each slice contains at most one partition of any segment, and
   * at most {@code targetSlices - 1} additional partitions are created in total.
   *
   * @param leaves index segments
   * @param targetSlices number of slices to create
   * @param minPartitionDocs minimum live docs in a partial segment partition
   * @param snapTolerance fraction of target slice size a cut may move to a segment boundary
   * @param parentBitSetProducer nested document parents, or null
   * @return index slices
   */
  public static LeafSlice[] balancedCutSlices(
      List<LeafReaderContext> leaves,
      int targetSlices,
      int minPartitionDocs,
      double snapTolerance,
      BitSetProducer parentBitSetProducer)
      throws IOException {
    List<SegmentDocs> segments = segmentDocs(leaves, parentBitSetProducer);
    if (segments.isEmpty()) {
      return new LeafSlice[0];
    }
    long totalDocs = 0;
    for (SegmentDocs segment : segments) {
      totalDocs += segment.numDocs;
    }
    int numSlices = (int) Math.max(1, Math.min(targetSlices, totalDocs / minPartitionDocs));
    double sliceDocs = (double) totalDocs / numSlices;
    long snapDocs = (long) (snapTolerance * sliceDocs);

    List<SliceAndSize> slices = new ArrayList<>(numSlices);
    List<LeafReaderContextPartition> current = new ArrayList<>();
    // live docs added to all completed slices and the current slice
    long assignedDocs = 0;
    long sliceStartDocs = 0;
    int sliceIndex = 1;

    for (SegmentDocs segment : segments) {
      int from = 0;
      while (from < segment.maxDoc) {
        int remaining = segment.liveDocs(from, segment.maxDoc);
        // where the next cut falls in global live doc space
        long cutPoint = Math.round(sliceIndex * sliceDocs);
        long toCut = cutPoint - assignedDocs;

        if (sliceIndex >= numSlices || remaining <= toCut + snapDocs) {
          // segment (remainder) fits in current slice, or snaps forward to its end
          current.add(segment.partition(from, segment.maxDoc));
          assignedDocs += remaining;
          from = segment.maxDoc;
          if (sliceIndex < numSlices && assignedDocs >= cutPoint - snapDocs) {
            slices.add(new SliceAndSize(new LeafSlice(current), assignedDocs - sliceStartDocs));
            current = new ArrayList<>();
            sliceStartDocs = assignedDocs;
            sliceIndex++;
          }
          continue;
        }

        if (toCut <= snapDocs && !current.isEmpty()) {
          // cut snaps back to the start of this segment (remainder)
          slices.add(new SliceAndSize(new LeafSlice(current), assignedDocs - sliceStartDocs));
          current = new ArrayList<>();
          sliceStartDocs = assignedDocs;
          sliceIndex++;
          continue;
        }

        // split this segment at the cut, keeping both sides above the minimum partition size
        long take = Math.max(toCut, minPartitionDocs);
        int cut;
        if (remaining - take < minPartitionDocs) {
          cut = segment.maxDoc;
        } else {
          cut = segment.findCut(from, take);
        }
        int taken = segment.liveDocs(from, cut);
        current.add(segment.partition(from, cut));
        assignedDocs += taken;
        from = cut;
        slices.add(new SliceAndSize(new LeafSlice(current), assignedDocs - sliceStartDocs));
        current = new ArrayList<>();
        sliceStartDocs = assignedDocs;
        sliceIndex++;
      }
    }
    if (!current.isEmpty()) {
      slices.add(new SliceAndSize(new LeafSlice(current), assignedDocs - sliceStartDocs));
    }
    return sortBySize(slices);
  }

  /**
   * Divide the index into slices by splitting each segment with more than {@code (1 +
   * snapTolerance) * targetSliceDocs} live docs into equal sized partitions, then packing all
   * segments and partitions into {@code targetSlices} slices, assigning the largest pieces first to
   * the slice with the fewest live docs. Partitions of the same segment are always placed in
   * different slices.
   *
   * @param leaves index segments
   * @param targetSlices number of slices to create
   * @param minPartitionDocs minimum live docs in a partial segment partition
   * @param snapTolerance fraction a segment may exceed the target slice size without being split
   * @param parentBitSetProducer nested document parents, or null
   * @return index slices
   */
  public static LeafSlice[] splitOutlierSlices(
      List<LeafReaderContext> leaves,
      int targetSlices,
      int minPartitionDocs,
      double snapTolerance,
      BitSetProducer parentBitSetProducer)
      throws IOException {
    List<SegmentDocs> segments = segmentDocs(leaves, parentBitSetProducer);
    if (segments.isEmpty()) {
      return new LeafSlice[0];
    }
    long totalDocs = 0;
    for (SegmentDocs segment : segments) {
      totalDocs += segment.numDocs;
    }
    int numSlices = (int) Math.max(1, Math.min(targetSlices, totalDocs / minPartitionDocs));
    double sliceDocs = (double) totalDocs / numSlices;

    record Piece(SegmentDocs segment, LeafReaderContextPartition partition, int numDocs) {}
    List<Piece> pieces = new ArrayList<>();
    for (SegmentDocs segment : segments) {
      int numPieces = 1;
      if (segment.numDocs > (1.0 + snapTolerance) * sliceDocs) {
        numPieces =
            (int)
                Math.min(
                    Math.min(numSlices, Math.ceil(segment.numDocs / sliceDocs)),
                    Math.max(1, segment.numDocs / minPartitionDocs));
      }
      int from = 0;
      int assigned = 0;
      for (int i = 1; i <= numPieces && from < segment.maxDoc; ++i) {
        int cut;
        if (i == numPieces) {
          cut = segment.maxDoc;
        } else {
          long target = Math.round((double) segment.numDocs * i / numPieces) - assigned;
          cut = segment.findCut(from, target);
        }
        int pieceDocs = segment.liveDocs(from, cut);
        pieces.add(new Piece(segment, segment.partition(from, cut), pieceDocs));
        assigned += pieceDocs;
        from = cut;
      }
    }
    pieces.sort(Comparator.comparingInt(Piece::numDocs).reversed());

    class Bin {
      final List<LeafReaderContextPartition> partitions = new ArrayList<>();
      final List<SegmentDocs> segments = new ArrayList<>();
      long numDocs = 0;
    }
    List<Bin> bins = new ArrayList<>(numSlices);
    for (int i = 0; i < numSlices; ++i) {
      bins.add(new Bin());
    }
    for (Piece piece : pieces) {
      // least loaded bin that does not already hold a partition of this segment, a segment has
      // at most numSlices pieces so one always exists
      Bin best = null;
      for (Bin bin : bins) {
        if ((best == null || bin.numDocs < best.numDocs) && !bin.segments.contains(piece.segment)) {
          best = bin;
        }
      }
      best.partitions.add(piece.partition);
      best.segments.add(piece.segment);
      best.numDocs += piece.numDocs;
    }

    List<SliceAndSize> slices = new ArrayList<>(numSlices);
    for (Bin bin : bins) {
      if (!bin.partitions.isEmpty()) {
        bin.partitions.sort(
            Comparator.<LeafReaderContextPartition>comparingInt(p -> p.ctx.docBase)
                .thenComparingInt(p -> p.minDocId));
        slices.add(new SliceAndSize(new LeafSlice(bin.partitions), bin.numDocs));
      }
    }
    return sortBySize(slices);
  }

  /**
   * Order slices by live docs, largest to smallest, so that the largest are submitted for execution
   * first.
   */
  private static LeafSlice[] sortBySize(List<SliceAndSize> slices) {
    slices.sort(Comparator.comparingLong((SliceAndSize ss) -> ss.numDocs).reversed());
    LeafSlice[] sorted = new LeafSlice[slices.size()];
    for (int i = 0; i < sorted.length; ++i) {
      sorted[i] = slices.get(i).slice;
    }
    return sorted;
  }

  /**
   * Get statistics about how evenly the given slices divide the index. Doc counts are live docs, so
   * deleted documents are not included.
   *
   * @param slices index slices
   * @return slice statistics
   */
  public static SliceStats getSliceStats(LeafSlice[] slices) throws IOException {
    if (slices.length == 0) {
      return new SliceStats(0, 0, 0, 0, 0, 0, 0, 1.0);
    }
    long totalDocs = 0;
    long minDocs = Long.MAX_VALUE;
    long maxDocs = 0;
    int partitions = 0;
    Map<LeafReaderContext, Integer> segmentPartitions = new HashMap<>();
    for (LeafSlice slice : slices) {
      long docs = 0;
      for (LeafReaderContextPartition partition : slice.partitions) {
        docs += partitionLiveDocs(partition);
        segmentPartitions.merge(partition.ctx, 1, Integer::sum);
      }
      totalDocs += docs;
      minDocs = Math.min(minDocs, docs);
      maxDocs = Math.max(maxDocs, docs);
      partitions += slice.partitions.length;
    }
    int splitSegments = (int) segmentPartitions.values().stream().filter(c -> c > 1).count();
    double meanDocs = (double) totalDocs / slices.length;
    return new SliceStats(
        slices.length,
        partitions,
        segmentPartitions.size(),
        splitSegments,
        totalDocs,
        minDocs,
        maxDocs,
        meanDocs == 0 ? 1.0 : maxDocs / meanDocs);
  }

  private static long partitionLiveDocs(LeafReaderContextPartition partition) throws IOException {
    int maxDoc = partition.ctx.reader().maxDoc();
    int to = Math.min(partition.maxDocId, maxDoc);
    if (partition.minDocId == 0 && to == maxDoc) {
      return partition.ctx.reader().numDocs();
    }
    return new SegmentDocs(partition.ctx, null).liveDocs(partition.minDocId, to);
  }

  /**
   * Statistics about the division of an index into slices. Doc counts are live docs.
   *
   * @param numSlices number of slices
   * @param numPartitions total number of partitions across all slices
   * @param numSegments number of distinct segments
   * @param numSplitSegments number of segments divided into multiple partitions
   * @param totalDocs live docs across all slices
   * @param minSliceDocs live docs in the smallest slice
   * @param maxSliceDocs live docs in the largest slice
   * @param imbalance ratio of the largest slice docs to the mean slice docs, 1.0 is perfectly even
   */
  public record SliceStats(
      int numSlices,
      int numPartitions,
      int numSegments,
      int numSplitSegments,
      long totalDocs,
      long minSliceDocs,
      long maxSliceDocs,
      double imbalance) {

    @Override
    public String toString() {
      return String.format(
          "slices=%d, partitions=%d, segments=%d, splitSegments=%d, docs=%d, minSliceDocs=%d,"
              + " maxSliceDocs=%d, imbalance=%.3f",
          numSlices,
          numPartitions,
          numSegments,
          numSplitSegments,
          totalDocs,
          minSliceDocs,
          maxSliceDocs,
          imbalance);
    }
  }
}
