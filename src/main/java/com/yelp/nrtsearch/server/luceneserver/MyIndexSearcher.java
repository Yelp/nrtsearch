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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.util.Bits;

/**
 * This is sadly necessary because for ToParentBlockJoinQuery we must invoke .scorer not
 * .bulkScorer, yet for DrillSideways we must do exactly the opposite!
 */
public class MyIndexSearcher extends IndexSearcher {

  /**
   * Class that uses an Executor implementation to hold the parallel search Executor and any
   * parameters needed to compute index search slices. This is hacky, but unfortunately necessary
   * since {@link IndexSearcher#slices(List)} is called directly from the constructor, which happens
   * before any member variable are set in the child class.
   */
  public static class ExecutorWithParams implements Executor {
    final Executor wrapped;
    final int sliceMaxDocs;
    final int sliceMaxSegments;
    final int virtualShards;

    /**
     * Constructor.
     *
     * @param wrapped executor to perform parallel search operations
     * @param sliceMaxDocs max docs per index slice
     * @param sliceMaxSegments max segments per index slice
     * @param virtualShards number for virtual shards for index
     * @throws NullPointerException if wrapped is null
     */
    public ExecutorWithParams(
        Executor wrapped, int sliceMaxDocs, int sliceMaxSegments, int virtualShards) {
      Objects.requireNonNull(wrapped);
      this.wrapped = wrapped;
      this.sliceMaxDocs = sliceMaxDocs;
      this.sliceMaxSegments = sliceMaxSegments;
      this.virtualShards = virtualShards;
    }

    @Override
    public void execute(Runnable command) {
      wrapped.execute(command);
    }

    @Override
    public String toString() {
      return String.format(
          "ExecutorWithParams(sliceMaxDocs=%d, sliceMaxSegments=%d, virtualShards=%d, wrapped=%s)",
          sliceMaxDocs, sliceMaxSegments, virtualShards, wrapped);
    }
  }

  /**
   * Constructor.
   *
   * @param reader index reader
   * @param executorWithParams parameter class that hold search executor and slice config
   */
  public MyIndexSearcher(IndexReader reader, ExecutorWithParams executorWithParams) {
    super(reader, executorWithParams);
  }

  /** * start segment to thread mapping * */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    if (!(getExecutor() instanceof ExecutorWithParams)) {
      throw new IllegalArgumentException("Executor must be an ExecutorWithParams");
    }
    ExecutorWithParams executorWithParams = (ExecutorWithParams) getExecutor();
    if (executorWithParams.virtualShards > 1) {
      return slicesForShards(
          leaves,
          executorWithParams.virtualShards,
          executorWithParams.sliceMaxDocs,
          executorWithParams.sliceMaxSegments);
    } else {
      return slices(leaves, executorWithParams.sliceMaxDocs, executorWithParams.sliceMaxSegments);
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
      for (int i = 0; i < slice.leaves.length; ++i) {
        numDocs += slice.leaves[i].reader().numDocs();
      }
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

  /* Better Segment To Thread Mapping Algorithm: https://issues.apache.org/jira/browse/LUCENE-8757
  This change is available in 9.0 (master) which is not released yet
  https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java#L316
  We can remove this method once luceneVersion is updated to 9.x
  * */

  /** Static method to segregate LeafReaderContexts amongst multiple slices */
  public static LeafSlice[] slices(
      List<LeafReaderContext> leaves, int maxDocsPerSlice, int maxSegmentsPerSlice) {
    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

    // Sort by maxDoc, descending:
    Collections.sort(
        sortedLeaves, Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
    long docSum = 0;
    List<LeafReaderContext> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
      if (ctx.reader().maxDoc() > maxDocsPerSlice) {
        assert group == null;
        groupedLeaves.add(Collections.singletonList(ctx));
      } else {
        if (group == null) {
          group = new ArrayList<>();
          group.add(ctx);

          groupedLeaves.add(group);
        } else {
          group.add(ctx);
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
    for (List<LeafReaderContext> currentLeaf : groupedLeaves) {
      // LeafSlice constructor has changed in 9.x. This allows to use old constructor.
      Collections.sort(currentLeaf, Comparator.comparingInt(l -> l.docBase));
      LeafReaderContext[] leavesArr = currentLeaf.toArray(new LeafReaderContext[0]);
      slices[upto] = new LeafSlice(leavesArr);
      ++upto;
    }

    return slices;
  }

  /** * end segment to thread mapping * */
  @Override
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector)
      throws IOException {
    boolean isDrillSidewaysQueryOrCompletionQuery =
        weight.getQuery() instanceof CompletionQuery
            || weight.getQuery().toString().contains("DrillSidewaysQuery");
    for (LeafReaderContext ctx : leaves) { // search each subreader
      // we force the use of Scorer (not BulkScorer) to make sure
      // that the scorer passed to LeafCollector.setScorer supports
      // Scorer.getChildren
      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(ctx);
      } catch (CollectionTerminatedException e) {
        // there is no doc of interest in this reader context
        // continue with the following leaf
        continue;
      }
      if (isDrillSidewaysQueryOrCompletionQuery) {
        BulkScorer scorer = weight.bulkScorer(ctx);
        if (scorer != null) {
          try {
            scorer.score(leafCollector, ctx.reader().getLiveDocs());
          } catch (CollectionTerminatedException e) {
            // collection was terminated prematurely
            // continue with the following leaf
          }
        }
      } else {
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
          leafCollector.setScorer(scorer);
          final Bits liveDocs = ctx.reader().getLiveDocs();
          final DocIdSetIterator it = scorer.iterator();
          try {
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
              if (liveDocs == null || liveDocs.get(doc)) {
                leafCollector.collect(doc);
              }
            }
          } catch (CollectionTerminatedException e) {
            // collection was terminated prematurely
            // continue with the following leaf
          }
        }
      }
    }
  }
}
