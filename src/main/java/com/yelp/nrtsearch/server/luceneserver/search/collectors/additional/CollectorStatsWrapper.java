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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult.AdditionalCollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.CollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.SearchStats;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Collector manager implementation that wraps collection by an {@link AdditionalCollectorManager}
 * and records profiling stats.
 *
 * @param <C> collector type
 * @param <R> collector result type
 */
public class CollectorStatsWrapper<C extends Collector, R extends CollectorResult>
    implements AdditionalCollectorManager<CollectorStatsWrapper<C, R>.StatsCollectorWrapper, R> {
  private final AdditionalCollectorManager<C, R> wrapped;
  private Collection<StatsCollectorWrapper> collectors;

  /**
   * Constructor.
   *
   * @param wrapped collector to record stats for
   */
  public CollectorStatsWrapper(AdditionalCollectorManager<C, R> wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public void setSearchContext(SearchContext searchContext) {
    wrapped.setSearchContext(searchContext);
  }

  @Override
  public StatsCollectorWrapper newCollector() throws IOException {
    return new StatsCollectorWrapper(wrapped.newCollector());
  }

  @Override
  public R reduce(Collection<CollectorStatsWrapper<C, R>.StatsCollectorWrapper> collectors)
      throws IOException {
    this.collectors = collectors;
    List<C> innerCollectors = new ArrayList<>(collectors.size());
    for (StatsCollectorWrapper collector : collectors) {
      innerCollectors.add(collector.collector);
    }
    return wrapped.reduce(innerCollectors);
  }

  /**
   * Add collector profiling stats to profile results builder.
   *
   * @param profileResultBuilder profile results builder
   * @throws NullPointerException if builder is null
   */
  public void addProfiling(ProfileResult.Builder profileResultBuilder) {
    Objects.requireNonNull(profileResultBuilder);

    if (collectors != null) {
      if (profileResultBuilder.getSearchStats().getCollectorStatsCount() == collectors.size()) {
        SearchStats.Builder searchStatsBuilder = profileResultBuilder.getSearchStatsBuilder();

        int index = 0;
        for (StatsCollectorWrapper statsWrapper : collectors) {
          CollectorStats.Builder collectorStatsBuilder =
              searchStatsBuilder.getCollectorStatsBuilder(index);
          collectorStatsBuilder.putAdditionalCollectorStats(
              getName(),
              AdditionalCollectorStats.newBuilder()
                  .setCollectTimeMs(statsWrapper.totalCollectTimeNs / 1000000.0)
                  .build());
          index++;
        }
      }
    }
  }

  /** Stats collector that wraps another collector. Records total time spent collecting. */
  class StatsCollectorWrapper implements Collector {

    private final C collector;
    private long totalCollectTimeNs = 0;

    public StatsCollectorWrapper(C collector) {
      this.collector = collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new StatsLeafCollectorWrapper(collector.getLeafCollector(context));
    }

    @Override
    public ScoreMode scoreMode() {
      return collector.scoreMode();
    }

    /**
     * Leaf collector that wraps another leaf collector. Adds collection duration for each doc to
     * the collector total.
     */
    class StatsLeafCollectorWrapper implements LeafCollector {
      private final LeafCollector leafCollector;

      public StatsLeafCollectorWrapper(LeafCollector leafCollector) {
        this.leafCollector = leafCollector;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafCollector.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        long start = System.nanoTime();
        leafCollector.collect(doc);
        long end = System.nanoTime();
        totalCollectTimeNs += (end - start);
      }
    }
  }
}
