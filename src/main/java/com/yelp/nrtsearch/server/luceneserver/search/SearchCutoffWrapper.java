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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;

/**
 * {@link CollectorManager} that wraps another manager to provide support for cutting short a search
 * operation under certain conditions, such as a timeout.
 *
 * <p>The timeout value is checked before processing each segment. If partial results are allowed, a
 * {@link CollectionTerminatedException} is thrown so the searcher moves to the next segment. If
 * partial results are not allowed, a {@link CollectionTimeoutException} is thrown. The timer starts
 * on the first call to {@link #newCollector()}.
 *
 * @param <C> collector type of wrapped manager
 * @param <T> top docs type of wrapped manager
 */
public class SearchCutoffWrapper<C extends Collector, T extends TopDocs>
    implements CollectorManager<SearchCutoffWrapper<C, T>.TimeoutCollectorWrapper, T> {

  private final CollectorManager<C, T> in;
  private final boolean noPartialResults;
  private final double timeoutSec;
  private final int checkEvery;
  private final Runnable onTimeout;
  private long maxTime = -1;

  /**
   * Constructor
   *
   * @param in manager to wrap
   * @param timeoutSec timeout for collecting
   * @param noPartialResults if a cutoff should be an exception, instead of using partial results
   * @param onTimeout an action to do if there was a timeout (done in reduce call)
   */
  public SearchCutoffWrapper(
      CollectorManager<C, T> in, double timeoutSec, boolean noPartialResults, Runnable onTimeout) {
    this(in, timeoutSec, 0, noPartialResults, onTimeout);
  }

  /**
   * Constructor
   *
   * @param in manager to wrap
   * @param timeoutSec timeout for collecting
   * @param checkEvery check timeout condition after each collection of this many document in a
   *     segment, 0 to only check on the segment boundary
   * @param noPartialResults if a cutoff should be an exception, instead of using partial results
   * @param onTimeout an action to do if there was a timeout (done in reduce call)
   */
  public SearchCutoffWrapper(
      CollectorManager<C, T> in,
      double timeoutSec,
      int checkEvery,
      boolean noPartialResults,
      Runnable onTimeout) {
    this.in = in;
    this.noPartialResults = noPartialResults;
    this.timeoutSec = timeoutSec;
    this.checkEvery = checkEvery;
    this.onTimeout = onTimeout;
  }

  // make this overridable for testing
  protected long getTimeMs() {
    return System.currentTimeMillis();
  }

  @Override
  public TimeoutCollectorWrapper newCollector() throws IOException {
    // start timer when first collector is created
    if (maxTime < 0) {
      maxTime = getTimeMs() + (long) (timeoutSec * 1000);
    }
    return new TimeoutCollectorWrapper(in.newCollector());
  }

  @Override
  public T reduce(Collection<SearchCutoffWrapper<C, T>.TimeoutCollectorWrapper> collectors)
      throws IOException {
    List<C> innerCollectors = new ArrayList<>(collectors.size());
    boolean hadTimeout = false;
    for (TimeoutCollectorWrapper collector : collectors) {
      innerCollectors.add(collector.collector);
      if (collector.hadTimeout) {
        hadTimeout = true;
      }
    }
    if (hadTimeout) {
      onTimeout.run();
    }
    return in.reduce(innerCollectors);
  }

  /**
   * {@link Collector} implementation that wraps another collector and checks if the operation has
   * timed out before providing each segment collector.
   */
  class TimeoutCollectorWrapper implements Collector {

    private final C collector;
    private boolean hadTimeout = false;

    public TimeoutCollectorWrapper(C collector) {
      this.collector = collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      checkTimeout();
      if (checkEvery > 0) {
        return new TimeoutLeafCollectorWrapper(collector.getLeafCollector(context));
      } else {
        return collector.getLeafCollector(context);
      }
    }

    @Override
    public ScoreMode scoreMode() {
      return collector.scoreMode();
    }

    private void checkTimeout() {
      if (getTimeMs() > maxTime) {
        hadTimeout = true;
        if (noPartialResults) {
          throw new CollectionTimeoutException(
              "Search collection exceeded timeout of " + timeoutSec + "s");
        } else {
          throw new CollectionTerminatedException();
        }
      }
    }

    /**
     * {@link LeafCollector} implementation that wraps another leaf collector and checks if the
     * operations has timed out during document collection. The timeout condition is checked every
     * time the configured number of documents are collected.
     */
    class TimeoutLeafCollectorWrapper implements LeafCollector {
      private final LeafCollector leafCollector;
      private int collectionCount = 0;

      public TimeoutLeafCollectorWrapper(LeafCollector leafCollector) {
        this.leafCollector = leafCollector;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafCollector.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        if (collectionCount == checkEvery) {
          collectionCount = 0;
          checkTimeout();
        }
        leafCollector.collect(doc);
        collectionCount++;
      }
    }
  }

  /** Exception to indicate collection during a search operation timed out. */
  public static class CollectionTimeoutException extends RuntimeException {
    public CollectionTimeoutException(String message) {
      super(message);
    }

    public CollectionTimeoutException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
