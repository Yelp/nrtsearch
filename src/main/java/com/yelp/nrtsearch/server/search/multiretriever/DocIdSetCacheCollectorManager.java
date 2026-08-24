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
package com.yelp.nrtsearch.server.search.multiretriever;

import com.yelp.nrtsearch.server.search.SearcherResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * A {@link CollectorManager} wrapper that intercepts document collection to capture doc IDs into a
 * per-retriever {@link DocIdSetCache.RetrieverBitSets}, while delegating all actual ranking and
 * collection to the wrapped manager.
 *
 * <p>Supports early exit: once the per-retriever hit count exceeds the threshold, the collector
 * stops writing to the bitset (just delegates). No atomic operations on the hot path — each leaf
 * collector holds a plain int counter via its {@link DocIdSetCache.RetrieverBitSets}.
 */
public class DocIdSetCacheCollectorManager
    implements CollectorManager<DocIdSetCacheCollectorManager.CachingCollector, SearcherResult> {

  private final CollectorManager<? extends Collector, SearcherResult> delegate;
  private final DocIdSetCache.RetrieverBitSets retrieverBitSets;

  public DocIdSetCacheCollectorManager(
      CollectorManager<? extends Collector, SearcherResult> delegate,
      DocIdSetCache.RetrieverBitSets retrieverBitSets) {
    this.delegate = delegate;
    this.retrieverBitSets = retrieverBitSets;
  }

  @Override
  public CachingCollector newCollector() throws IOException {
    Collector delegateCollector = delegate.newCollector();
    return new CachingCollector(delegateCollector);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SearcherResult reduce(Collection<CachingCollector> collectors) throws IOException {
    ArrayList<Collector> delegateCollectors = new ArrayList<>(collectors.size());
    for (CachingCollector cachingCollector : collectors) {
      delegateCollectors.add(cachingCollector.getDelegate());
    }
    return ((CollectorManager<Collector, SearcherResult>) delegate).reduce(delegateCollectors);
  }

  /** Wraps a delegate collector and records collected doc IDs into the retriever's bitsets. */
  public class CachingCollector implements Collector {

    private final Collector delegate;

    CachingCollector(Collector delegate) {
      this.delegate = delegate;
    }

    public Collector getDelegate() {
      return delegate;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      LeafCollector delegateLeafCollector = delegate.getLeafCollector(context);
      if (retrieverBitSets.isExceeded()) {
        return delegateLeafCollector;
      }
      SparseFixedBitSet bitSet =
          retrieverBitSets.getOrCreateBitSet(context.ord, context.reader().maxDoc());
      return new CachingLeafCollector(delegateLeafCollector, bitSet);
    }

    @Override
    public ScoreMode scoreMode() {
      return delegate.scoreMode();
    }
  }

  private class CachingLeafCollector implements LeafCollector {

    private final LeafCollector delegate;
    private final SparseFixedBitSet bitSet;

    CachingLeafCollector(LeafCollector delegate, SparseFixedBitSet bitSet) {
      this.delegate = delegate;
      this.bitSet = bitSet;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (!retrieverBitSets.isExceeded()) {
        bitSet.set(doc);
        retrieverBitSets.recordHit();
      }
      delegate.collect(doc);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      delegate.setScorer(scorer);
    }
  }
}
