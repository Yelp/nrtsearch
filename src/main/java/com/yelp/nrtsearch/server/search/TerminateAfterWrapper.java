/*
 * Copyright 2021 Yelp Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;

/**
 * Collector manager wrapper that terminates collection after a given number of documents. This doc
 * limit is global and shared between all parallel collectors.
 *
 * @param <C> collector type of wrapped manager
 */
public class TerminateAfterWrapper<C extends Collector>
    implements CollectorManager<
        TerminateAfterWrapper<C>.TerminateAfterCollectorWrapper, SearcherResult> {

  private final CollectorManager<C, SearcherResult> in;
  private final int terminateAfter;
  private final int terminateAfterMaxRecallCount;
  private final Runnable onEarlyTerminate;
  private final AtomicInteger collectedDocCount;

  /**
   * Constructor.
   *
   * @param in manager to wrap
   * @param terminateAfter maximum number of documents to collect
   * @param onEarlyTerminate action to perform if collection terminated early (done in reduce call)
   */
  public TerminateAfterWrapper(
      CollectorManager<C, SearcherResult> in,
      int terminateAfter,
      int terminateAfterMaxRecallCount,
      Runnable onEarlyTerminate) {
    this.in = in;
    this.terminateAfter = terminateAfter;
    this.terminateAfterMaxRecallCount = terminateAfterMaxRecallCount;
    this.onEarlyTerminate = onEarlyTerminate;
    this.collectedDocCount = new AtomicInteger();
  }

  @Override
  public TerminateAfterCollectorWrapper newCollector() throws IOException {
    return new TerminateAfterCollectorWrapper(in.newCollector());
  }

  @Override
  public SearcherResult reduce(Collection<TerminateAfterCollectorWrapper> collectors)
      throws IOException {
    List<C> innerCollectors = new ArrayList<>(collectors.size());
    boolean didTerminateEarly = false;
    for (TerminateAfterCollectorWrapper collector : collectors) {
      innerCollectors.add(collector.collector);
      if (collector.terminatedEarly) {
        didTerminateEarly = true;
      }
    }
    SearcherResult searcherResult = in.reduce(innerCollectors);
    if (didTerminateEarly) {
      onEarlyTerminate.run();
      int totalHits = 0;
      for (TerminateAfterCollectorWrapper collector : collectors) {
        totalHits += collector.docCount;
      }
      searcherResult.getTopDocs().totalHits =
          new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }
    return searcherResult;
  }

  /** Get the collector manager being wrapped. */
  public CollectorManager<C, SearcherResult> getWrapped() {
    return in;
  }

  /** Max documents to collect. */
  public int getTerminateAfter() {
    return terminateAfter;
  }

  /**
   * {@link Collector} implementation that wraps another collector and terminates collection after a
   * certain global count of documents is reached.
   */
  public class TerminateAfterCollectorWrapper implements Collector {

    private final C collector;
    private boolean terminatedEarly = false;
    private int docCount = 0;

    public TerminateAfterCollectorWrapper(C collector) {
      this.collector = collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      if (terminatedEarly) {
        throw new CollectionTerminatedException();
      }
      return new TerminateAfterLeafCollectorWrapper(collector.getLeafCollector(context));
    }

    @Override
    public ScoreMode scoreMode() {
      return collector.scoreMode();
    }

    /**
     * {@link LeafCollector} implementation that wraps another leaf collector and checks a global
     * counter before each doc is collected to terminate collection early when a desired number is
     * reached.
     */
    public class TerminateAfterLeafCollectorWrapper implements LeafCollector {
      private final LeafCollector leafCollector;

      public TerminateAfterLeafCollectorWrapper(LeafCollector leafCollector) {
        this.leafCollector = leafCollector;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafCollector.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        int currDocCount = collectedDocCount.incrementAndGet();
        if (currDocCount > terminateAfter) {
          terminatedEarly = true;
          if (currDocCount > terminateAfterMaxRecallCount) {
            throw new CollectionTerminatedException();
          }
          docCount++;
          return;
        }
        leafCollector.collect(doc);
        docCount++;
      }
    }
  }
}
