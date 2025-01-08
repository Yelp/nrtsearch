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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;

/**
 * Custom IndexSearcher that allows for custom slicing of the index into multiple segments for
 * parallel search.
 */
public class MyIndexSearcher extends IndexSearcher {

  public record SlicingParams(int sliceMaxDocs, int sliceMaxSegments, int virtualShards) {}

  private static final Object slicingLock = new Object();
  private static SlicingParams staticSlicingParams;

  private SlicingParams slicingParams;

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
    // Use lock to serialize initialization of index searchers. The slicing params can only be
    // passed in the static
    // context. To allow for different configuration per index, we use a single static variable to
    // hold the slicing
    // params, which is read only during construction of the index searcher.
    synchronized (slicingLock) {
      staticSlicingParams = slicingParams;
      return new MyIndexSearcher(reader, executor);
    }
  }

  /**
   * Constructor.
   *
   * @param reader index reader
   * @param executor parallel search task executor
   */
  protected MyIndexSearcher(IndexReader reader, Executor executor) {
    super(reader, executor);
  }

  @VisibleForTesting
  SlicingParams getSlicingParams() {
    return slicingParams;
  }

  /** * start segment to thread mapping * */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    slicingParams = staticSlicingParams;
    if (slicingParams == null) {
      throw new IllegalArgumentException("Slicing params not set");
    }
    if (slicingParams.virtualShards > 1) {
      return slicesForShards(
          leaves,
          slicingParams.virtualShards,
          slicingParams.sliceMaxDocs,
          slicingParams.sliceMaxSegments);
    } else {
      return slices(leaves, slicingParams.sliceMaxDocs, slicingParams.sliceMaxSegments);
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
      this.slice = slice;
      numDocs = slice.getMaxDocs();
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
}
