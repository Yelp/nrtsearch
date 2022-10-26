/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;

public class MyTopSuggestDocsCollector extends DocCollector {
  private static final Comparator<TopSuggestDocs.SuggestScoreDoc> SUGGEST_SCORE_DOC_COMPARATOR =
      (a, b) -> {
        // sort by higher score
        int cmp = Float.compare(b.score, a.score);
        if (cmp == 0) {
          // tie-break by completion key
          return Lookup.CHARSEQUENCE_COMPARATOR.compare(a.key, b.key);
        }
        return cmp;
      };

  private final MyTopSuggestDocsCollectorManager manager;

  public MyTopSuggestDocsCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    super(context, additionalCollectors);
    if (!additionalCollectors.isEmpty()) {
      throw new IllegalArgumentException("additionalCollectors must be empty");
    }
    manager = new MyTopSuggestDocsCollectorManager(getNumHitsToCollect());
  }

  @Override
  public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
    return manager;
  }

  @Override
  public void fillHitRanking(SearchResponse.Hit.Builder hitResponse, ScoreDoc scoreDoc) {
    if (!Float.isNaN(scoreDoc.score)) {
      hitResponse.setScore(scoreDoc.score);
    }
    if (scoreDoc instanceof TopSuggestDocs.SuggestScoreDoc) {
      // set the retrieval key from the SuggestScoreDoc to the hit response as "__suggest_key" field
      hitResponse.putFields(
          "__suggest_key",
          SearchResponse.Hit.CompositeFieldValue.newBuilder()
              .addFieldValue(
                  SearchResponse.Hit.FieldValue.newBuilder()
                      .setTextValue(((TopSuggestDocs.SuggestScoreDoc) scoreDoc).key.toString())
                      .build())
              .build());
    }
  }

  @Override
  public void fillLastHit(SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit) {
    stateBuilder.setLastScore(lastHit.score);
  }

  public static class MyTopSuggestDocsCollectorManager
      implements CollectorManager<TopSuggestDocsCollector, TopSuggestDocs> {
    private final int numHitsToCollect;

    public MyTopSuggestDocsCollectorManager(int numHitsToCollect) {
      this.numHitsToCollect = numHitsToCollect;
    }

    @Override
    public TopSuggestDocsCollector newCollector() throws IOException {
      return new ExhaustiveTopSuggestDocsCollector(this.numHitsToCollect);
    }

    /**
     * Merges results from multiple collectors to a single TopSuggestDocs. Keeps the highest score
     * SuggestScoreDoc objects, unique per docId and limited to `numHitsToCollect`.
     *
     * @param collectors collection of collectors
     * @return final TopSuggestDocs, containing the top (my score) SuggestScoreDoc results, unique
     *     by docId
     * @throws IOException on error collecting results from collector
     */
    @Override
    public TopSuggestDocs reduce(Collection<TopSuggestDocsCollector> collectors)
        throws IOException {
      final List<TopSuggestDocs.SuggestScoreDoc> topDocs = new LinkedList<>();
      for (TopSuggestDocsCollector collector : collectors) {
        topDocs.addAll(List.of(collector.get().scoreLookupDocs()));
      }

      // retrieve the top this.numHitsToCollect suggestScoreDocs, sorting them by score
      TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocs =
          topDocs.stream()
              .sorted(SUGGEST_SCORE_DOC_COMPARATOR) // highest score first
              .limit(this.numHitsToCollect)
              .toArray(TopSuggestDocs.SuggestScoreDoc[]::new);

      return new TopSuggestDocs(
          new TotalHits(this.numHitsToCollect, TotalHits.Relation.EQUAL_TO), suggestScoreDocs);
    }
  }

  public static class ExhaustiveTopSuggestDocsCollector extends TopSuggestDocsCollector {
    private final SuggestScoreDocPriorityQueue priorityQueue;

    public ExhaustiveTopSuggestDocsCollector(int num) {
      super(num, false);
      priorityQueue = new SuggestScoreDocPriorityQueue(num);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) {
      docBase = context.docBase;
    }

    @Override
    public void collect(int docID, CharSequence key, CharSequence context, float score)
        throws IOException {
      TopSuggestDocs.SuggestScoreDoc suggestScoreDoc =
          new TopSuggestDocs.SuggestScoreDoc(docBase + docID, key, context, score);
      priorityQueue.add(suggestScoreDoc);
    }

    @Override
    public TopSuggestDocs get() throws IOException {
      TopSuggestDocs.SuggestScoreDoc[] results = priorityQueue.getResults();
      if (results.length > 0) {
        return new TopSuggestDocs(
            new TotalHits(results.length, TotalHits.Relation.EQUAL_TO), results);
      } else {
        return TopSuggestDocs.EMPTY;
      }
    }
  }

  public static class SuggestScoreDocPriorityQueue {
    private final Map<Integer, TopSuggestDocs.SuggestScoreDoc> docIdsMap;
    private final PriorityQueue<TopSuggestDocs.SuggestScoreDoc> priorityQueue;

    public SuggestScoreDocPriorityQueue(int num) {
      docIdsMap = new HashMap<>(num);
      priorityQueue = new PriorityQueue<>(num, SUGGEST_SCORE_DOC_COMPARATOR);
    }

    /**
     * Adds the suggestScoreDoc to the queue on the condition that: 1. SuggestScoreDoc with the same
     * docId and greater score has not been collected before 2. The suggestScoreDoc's score is
     * higher than the lowest score previously collected
     *
     * @param suggestScoreDoc document to add to the queue
     * @return `true` if the operation changed the state of the queue, `false` otherwise
     */
    public boolean add(TopSuggestDocs.SuggestScoreDoc suggestScoreDoc) {
      if (!docIdsMap.containsKey(suggestScoreDoc.doc)
          || docIdsMap.get(suggestScoreDoc.doc).score < suggestScoreDoc.score) {
        TopSuggestDocs.SuggestScoreDoc previousValue =
            docIdsMap.put(suggestScoreDoc.doc, suggestScoreDoc);
        if (previousValue != null) {
          // remove old value from priority queue
          priorityQueue.remove(previousValue);
        }
        // add new value to priority queue
        priorityQueue.add(suggestScoreDoc);
      }
      return false;
    }

    /**
     * Poll all documents from the priority queue sorted by score. This method also clears the
     * queue.
     *
     * @return sorted results by score
     */
    public TopSuggestDocs.SuggestScoreDoc[] getResults() {
      int size = priorityQueue.size();
      TopSuggestDocs.SuggestScoreDoc[] res = new TopSuggestDocs.SuggestScoreDoc[size];
      for (int i = size - 1; i >= 0; i--) {
        res[i] = priorityQueue.poll();
      }
      docIdsMap.clear();
      return res;
    }
  }
}
