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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;
import org.apache.lucene.util.Bits;

/**
 * Primary {@link CollectorManager} used for search. Wraps collection of top documents and any
 * additional collectors. The document collection is provided by a {@link DocCollector}
 * implementation. Additional collection is provided by {@link AdditionalCollectorManager}
 * implementations.
 */
public class SearchCollectorManager
    implements CollectorManager<SearchCollectorManager.SearchCollector, SearcherResult> {

  private final DocCollector docCollector;
  private final CollectorManager<? extends Collector, ? extends TopDocs> docCollectorManger;
  private final List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
      collectorManagers;

  /**
   * Constructor.
   *
   * @param docCollector collection info for getting the top documents
   * @param collectorManagers additional collectors
   */
  public SearchCollectorManager(
      DocCollector docCollector,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          collectorManagers) {
    this.docCollector = docCollector;
    this.docCollectorManger = docCollector.getManager();
    this.collectorManagers = collectorManagers;
  }

  @Override
  public SearchCollector newCollector() throws IOException {
    return new SearchCollector();
  }

  @Override
  public SearcherResult reduce(Collection<SearchCollector> collectors) throws IOException {
    Collection<Collector> subCollectors = new ArrayList<>(collectors.size());
    for (SearchCollector searchCollector : collectors) {
      subCollectors.add(searchCollector.hitCollector);
    }

    // We must cast the CollectorManager to use the concrete Collector type. This will be
    // safe since the Collector object will be the one returned from the manager's
    // newCollector() method.
    @SuppressWarnings("unchecked")
    TopDocs hits =
        ((CollectorManager<Collector, ? extends TopDocs>) docCollectorManger).reduce(subCollectors);

    Map<String, CollectorResult> collectorResults;
    if (collectorManagers.isEmpty()) {
      collectorResults = Collections.emptyMap();
    } else {
      collectorResults = new HashMap<>();
      for (int i = 0; i < collectorManagers.size(); ++i) {
        subCollectors.clear();
        for (SearchCollector searchCollector : collectors) {
          subCollectors.add(searchCollector.additionalCollectors.get(i));
        }

        AdditionalCollectorManager<? extends Collector, ? extends CollectorResult> cm =
            collectorManagers.get(i);

        // We must cast the CollectorManager to use the concrete Collector type. See above comment.
        @SuppressWarnings("unchecked")
        CollectorResult result =
            ((AdditionalCollectorManager<Collector, ? extends CollectorResult>) cm)
                .reduce(subCollectors);
        collectorResults.put(cm.getName(), result);
      }
    }
    return new SearcherResult(hits, collectorResults);
  }

  /** Get collector info for retrieving query top docs */
  public DocCollector getDocCollector() {
    return docCollector;
  }

  /** Get collector manager implementation used to rank docs */
  public CollectorManager<? extends Collector, ? extends TopDocs> getDocCollectorManger() {
    return docCollectorManger;
  }

  /** Collector level class. There will be one Collector for each query thread. */
  public class SearchCollector implements Collector {

    private final Collector hitCollector;
    private final List<Collector> additionalCollectors;

    private SearchCollector() throws IOException {
      this.hitCollector = docCollectorManger.newCollector();
      if (collectorManagers.isEmpty()) {
        additionalCollectors = Collections.emptyList();
      } else {
        additionalCollectors = new ArrayList<>(collectorManagers.size());
        for (var collectorManager : collectorManagers) {
          additionalCollectors.add(collectorManager.newCollector());
        }
      }
    }

    /**
     * Note on instanceof TopSuggestDocsCollector: {@link
     * org.apache.lucene.search.suggest.document.CompletionScorer#score(LeafCollector, Bits)}, which
     * is used for {@link org.apache.lucene.search.suggest.document.CompletionQuery} requires
     * collector to be an instance of TopSuggestDocsCollector
     */
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      if (this.hitCollector instanceof TopSuggestDocsCollector) {
        return this.hitCollector.getLeafCollector(context);
      }
      return new SearchLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      ScoreMode scoreMode = hitCollector.scoreMode();
      for (Collector collector : additionalCollectors) {
        if (scoreMode == null) {
          scoreMode = collector.scoreMode();
        } else if (scoreMode != collector.scoreMode()) {
          return ScoreMode.COMPLETE;
        }
      }
      return scoreMode;
    }

    /** Segment level class. */
    public class SearchLeafCollector implements LeafCollector {

      private final LeafCollector hitLeafCollector;
      private final List<LeafCollector> additionalLeafCollectors;

      private SearchLeafCollector(LeafReaderContext context) throws IOException {
        hitLeafCollector = hitCollector.getLeafCollector(context);
        if (additionalCollectors.isEmpty()) {
          additionalLeafCollectors = Collections.emptyList();
        } else {
          additionalLeafCollectors = new ArrayList<>(additionalCollectors.size());
          for (Collector collector : additionalCollectors) {
            additionalLeafCollectors.add(collector.getLeafCollector(context));
          }
        }
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        hitLeafCollector.setScorer(scorer);
        for (LeafCollector leafCollector : additionalLeafCollectors) {
          leafCollector.setScorer(scorer);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        hitLeafCollector.collect(doc);
        for (LeafCollector leafCollector : additionalLeafCollectors) {
          leafCollector.collect(doc);
        }
      }
    }
  }
}
