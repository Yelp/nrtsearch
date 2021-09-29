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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult.CollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.SearchStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.SegmentStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * CollectorManager that wraps another manager and collects timing stats for the search execution.
 * Records timing for collection and reduce operations, as well as detailed stats for each processed
 * segment.
 *
 * @param <C> collector type of wrapped manager
 */
public class SearchStatsWrapper<C extends Collector>
    implements CollectorManager<SearchStatsWrapper<C>.SearchStatsCollectorWrapper, SearcherResult> {

  private Collection<SearchStatsCollectorWrapper> collectors;
  private final CollectorManager<C, SearcherResult> in;
  long collectStartNano = -1;
  long collectEndNano = -1;
  long reduceStartNano = -1;
  long reduceEndNano = -1;

  /**
   * Constructor
   *
   * @param in manager to wrap
   */
  public SearchStatsWrapper(CollectorManager<C, SearcherResult> in) {
    this.in = in;
  }

  @Override
  public SearchStatsCollectorWrapper newCollector() throws IOException {
    if (collectStartNano < 0) {
      collectStartNano = System.nanoTime();
      collectEndNano = collectStartNano;
    }
    return new SearchStatsCollectorWrapper(in.newCollector());
  }

  @Override
  public SearcherResult reduce(
      Collection<SearchStatsWrapper<C>.SearchStatsCollectorWrapper> collectors) throws IOException {
    this.collectors = collectors;
    collectEndNano = System.nanoTime();

    List<C> innerCollectors = new ArrayList<>(collectors.size());
    for (SearchStatsCollectorWrapper collector : collectors) {
      innerCollectors.add(collector.collector);
    }

    reduceStartNano = System.nanoTime();
    try {
      return in.reduce(innerCollectors);
    } finally {
      reduceEndNano = System.nanoTime();
    }
  }

  /** Get the collector manager being wrapped. */
  public CollectorManager<C, SearcherResult> getWrapped() {
    return in;
  }

  /**
   * Add search profiling stats to profile results builder.
   *
   * @param profileResultBuilder profile results builder
   * @throws NullPointerException if builder is null
   */
  public void addProfiling(ProfileResult.Builder profileResultBuilder) {
    Objects.requireNonNull(profileResultBuilder);

    // stats collection is incomplete
    if (collectStartNano == -1
        || collectEndNano == -1
        || reduceStartNano == -1
        || reduceEndNano == -1
        || this.collectors == null) {
      return;
    }

    SearchStats.Builder searchStatsBuilder = SearchStats.newBuilder();
    searchStatsBuilder.setTotalCollectTimeMs((collectEndNano - collectStartNano) / 1000000.0);
    searchStatsBuilder.setTotalReduceTimeMs((reduceEndNano - reduceStartNano) / 1000000.0);
    for (SearchStatsCollectorWrapper collector : collectors) {
      CollectorStats.Builder collectorStatsBuilder = CollectorStats.newBuilder();
      collectorStatsBuilder.setTerminated(collector.terminated);
      int totalCollected = 0;
      double totalCollectTimeMs = 0;
      for (SearchStatsLeafCollectorWrapper leafCollector : collector.leafCollectors) {
        SegmentStats.Builder segmentStatsBuilder = SegmentStats.newBuilder();
        segmentStatsBuilder.setMaxDoc(leafCollector.context.reader().maxDoc());
        segmentStatsBuilder.setNumDocs(leafCollector.context.reader().numDocs());
        segmentStatsBuilder.setCollectedCount(leafCollector.collectedCount);
        totalCollected += leafCollector.collectedCount;
        segmentStatsBuilder.setRelativeStartTimeMs(
            (leafCollector.leafStartNano - collectStartNano) / 1000000.0);
        double collectTimeMs =
            (leafCollector.leafEndNano - leafCollector.leafStartNano) / 1000000.0;
        segmentStatsBuilder.setCollectTimeMs(collectTimeMs);
        totalCollectTimeMs += collectTimeMs;
        collectorStatsBuilder.addSegmentStats(segmentStatsBuilder.build());
      }
      collectorStatsBuilder.setTotalCollectedCount(totalCollected);
      collectorStatsBuilder.setTotalCollectTimeMs(totalCollectTimeMs);
      searchStatsBuilder.addCollectorStats(collectorStatsBuilder.build());
    }
    profileResultBuilder.setSearchStats(searchStatsBuilder);
  }

  /**
   * Stats collector that wraps another collector. Records if collection was gracefully terminated.
   */
  class SearchStatsCollectorWrapper implements Collector {

    private final C collector;
    private final List<SearchStatsLeafCollectorWrapper> leafCollectors = new ArrayList<>();
    boolean terminated = false;

    public SearchStatsCollectorWrapper(C collector) {
      this.collector = collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      SearchStatsLeafCollectorWrapper leafCollector;
      try {
        leafCollector =
            new SearchStatsLeafCollectorWrapper(collector.getLeafCollector(context), context);
      } catch (CollectionTerminatedException e) {
        terminated = true;
        throw e;
      }

      leafCollectors.add(leafCollector);
      return leafCollector;
    }

    @Override
    public ScoreMode scoreMode() {
      return collector.scoreMode();
    }
  }

  /**
   * Leaf collector that wraps another leaf collector. Records the start and end of collection, and
   * the total collected documents.
   */
  static class SearchStatsLeafCollectorWrapper implements LeafCollector {

    private final LeafCollector in;
    private final LeafReaderContext context;
    int collectedCount = 0;
    final long leafStartNano;
    long leafEndNano;

    public SearchStatsLeafCollectorWrapper(LeafCollector in, LeafReaderContext context) {
      this.in = in;
      this.context = context;
      leafStartNano = System.nanoTime();
      leafEndNano = leafStartNano;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      in.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      collectedCount++;
      in.collect(doc);
      // update end time after each collection, since we don't know when it will be the last one
      leafEndNano = System.nanoTime();
    }
  }
}
