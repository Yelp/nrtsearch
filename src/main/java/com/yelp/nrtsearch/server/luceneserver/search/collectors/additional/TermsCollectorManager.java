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

import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.BucketResult.Bucket;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Collector manager that aggregates terms into buckets. Currently only supports terms generated
 * from a {@link FacetScript}.
 */
public class TermsCollectorManager
    implements AdditionalCollectorManager<TermsCollectorManager.TermsCollector, CollectorResult> {
  private final String name;
  private final int size;
  private final FacetScript.SegmentFactory scriptFactory;
  private SearchContext searchContext;

  /**
   * Constructor.
   *
   * @param name Collection name from request
   * @param grpcTermsCollector Collector parameters from request
   * @param context context info for collector building
   */
  public TermsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context) {
    this.name = name;
    this.size = grpcTermsCollector.getSize();

    switch (grpcTermsCollector.getTermsSourceCase()) {
      case SCRIPT:
        FacetScript.Factory factory =
            ScriptService.getInstance()
                .compile(grpcTermsCollector.getScript(), FacetScript.CONTEXT);
        scriptFactory =
            factory.newFactory(
                ScriptParamsUtils.decodeParams(grpcTermsCollector.getScript().getParamsMap()),
                context.getIndexState().docLookup);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported terms source: " + grpcTermsCollector.getTermsSourceCase());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setSearchContext(SearchContext searchContext) {}

  @Override
  public TermsCollector newCollector() throws IOException {
    return new TermsCollector();
  }

  @Override
  public CollectorResult reduce(Collection<TermsCollector> collectors) throws IOException {
    Map<Object, Integer> combinedCounts = combineCounts(collectors);
    BucketResult.Builder bucketBuilder = BucketResult.newBuilder();
    fillBucketResult(bucketBuilder, combinedCounts);

    return CollectorResult.newBuilder().setBucketResult(bucketBuilder.build()).build();
  }

  /** Combine term counts from each parallel collector into a single map */
  private Map<Object, Integer> combineCounts(Collection<TermsCollector> collectors) {
    if (collectors.isEmpty()) {
      return Collections.emptyMap();
    }
    Iterator<TermsCollector> iterator = collectors.iterator();
    TermsCollector termsCollector = iterator.next();
    Map<Object, Integer> totalCountsMap = termsCollector.countsMap;
    while (iterator.hasNext()) {
      termsCollector = iterator.next();
      for (Map.Entry<Object, Integer> entry : termsCollector.countsMap.entrySet()) {
        totalCountsMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
      }
    }
    return totalCountsMap;
  }

  /** Fill bucket result message based on collected term counts. */
  private void fillBucketResult(BucketResult.Builder bucketBuilder, Map<Object, Integer> counts) {
    if (counts.size() > 0 && size > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<Entry<Object, Integer>> priorityQueue =
          new PriorityQueue<>(
              Math.min(counts.size(), size), Map.Entry.comparingByValue(Integer::compare));

      int otherCounts = 0;
      int minimumCount = -1;
      for (Map.Entry<Object, Integer> entry : counts.entrySet()) {
        if (priorityQueue.size() < size) {
          priorityQueue.offer(entry);
          minimumCount = priorityQueue.peek().getValue();
        } else if (entry.getValue() > minimumCount) {
          otherCounts += priorityQueue.poll().getValue();
          priorityQueue.offer(entry);
          minimumCount = priorityQueue.peek().getValue();
        } else {
          otherCounts += entry.getValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<Bucket> buckets = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        Map.Entry<Object, Integer> entry = priorityQueue.poll();
        buckets.addFirst(
            Bucket.newBuilder()
                .setKey(entry.getKey().toString())
                .setCount(entry.getValue())
                .build());
      }
      bucketBuilder
          .addAllBuckets(buckets)
          .setTotalBuckets(counts.size())
          .setTotalOtherCounts(otherCounts);
    }
  }

  /** Collector implementation to record term counts generated by a {@link FacetScript}. */
  public class TermsCollector implements Collector {

    Map<Object, Integer> countsMap = new HashMap<>();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new TermsLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      // Script cannot currently use score
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    /** Leaf Collector implementation to record term counts generated by a {@link FacetScript}. */
    public class TermsLeafCollector implements LeafCollector {
      final FacetScript facetScript;

      public TermsLeafCollector(LeafReaderContext leafContext) throws IOException {
        facetScript = scriptFactory.newInstance(leafContext);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        // TODO make score available to script
      }

      @Override
      public void collect(int doc) throws IOException {
        facetScript.setDocId(doc);
        Object scriptResult = facetScript.execute();
        if (scriptResult != null) {
          processScriptResult(scriptResult, countsMap);
        }
      }

      private void processScriptResult(Object scriptResult, Map<Object, Integer> countsMap) {
        if (scriptResult instanceof Iterable) {
          ((Iterable<?>) scriptResult)
              .forEach(
                  v -> {
                    if (v != null) {
                      countsMap.merge(v, 1, Integer::sum);
                    }
                  });
        } else {
          countsMap.merge(scriptResult, 1, Integer::sum);
        }
      }
    }
  }
}
