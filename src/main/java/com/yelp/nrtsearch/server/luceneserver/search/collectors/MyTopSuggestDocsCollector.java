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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.TopSuggestDocs;
import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;
import org.apache.lucene.search.suggest.document.TopSuggestDocsCollector;

public class MyTopSuggestDocsCollector extends DocCollector {
  public static final String SUGGEST_KEY_FIELD_NAME = "__suggest_key";
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
          SUGGEST_KEY_FIELD_NAME,
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
      return new TerminatingTopSuggestDocsCollector(this.numHitsToCollect);
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
      TopSuggestDocs[] collectorTopDocs = new TopSuggestDocs[collectors.size()];
      int index = 0;
      for (TopSuggestDocsCollector collector : collectors) {
        collectorTopDocs[index] = collector.get();
        index++;
      }
      return TopSuggestDocs.merge(this.numHitsToCollect, collectorTopDocs);
    }
  }

  public static class TerminatingTopSuggestDocsCollector extends TopSuggestDocsCollector {
    private final SuggestScoreDocPriorityQueue priorityQueue;

    public TerminatingTopSuggestDocsCollector(int num) {
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
      if (priorityQueue.insertWithOverflow(suggestScoreDoc) == suggestScoreDoc) {
        // This score doc is less than the lowest scoring doc in the priority queue.
        // As collection is done in score order, we don't need to consider any more
        // documents in this segment.
        throw new CollectionTerminatedException();
      }
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
    private final int maxSize;

    public SuggestScoreDocPriorityQueue(int maxSize) {
      this.maxSize = maxSize;
      docIdsMap = new HashMap<>();
      // min heap
      priorityQueue = new PriorityQueue<>(maxSize, SUGGEST_SCORE_DOC_COMPARATOR.reversed());
    }

    /**
     * Insert a {@link SuggestScoreDoc} into the priority queue and return the displaced doc, if
     * any. If there is already an entry in the queue for this doc id, the new doc will replace the
     * old if its sort order is higher.
     *
     * @param suggestScoreDoc doc to insert into queue.
     * @return doc in the queue the new entry displaced (could be provided doc if lowest scoring),
     *     or null
     */
    public TopSuggestDocs.SuggestScoreDoc insertWithOverflow(
        TopSuggestDocs.SuggestScoreDoc suggestScoreDoc) {
      TopSuggestDocs.SuggestScoreDoc previousForId =
          docIdsMap.put(suggestScoreDoc.doc, suggestScoreDoc);
      // if there is a previous entry for this id, remove if it sorts lower
      if (previousForId != null) {
        if (isLessThan(suggestScoreDoc, previousForId)) {
          // no need to insert this doc, but return null because collection may not be finished
          return null;
        } else {
          priorityQueue.remove(previousForId);
        }
      }

      if (priorityQueue.size() < maxSize) {
        priorityQueue.add(suggestScoreDoc);
        // this doc may have displaced a previous one for this id
        return previousForId;
      } else if (priorityQueue.size() > 0 && isLessThan(priorityQueue.peek(), suggestScoreDoc)) {
        // remove the current lowest doc and replace with higher scoring new doc
        SuggestScoreDoc previous = priorityQueue.poll();
        priorityQueue.add(suggestScoreDoc);
        return previous;
      } else {
        return suggestScoreDoc;
      }
    }

    /**
     * Get if the first suggest doc should be sorted after the second
     *
     * @param s1 first doc
     * @param s2 second doc
     * @return if first < second
     */
    private boolean isLessThan(SuggestScoreDoc s1, SuggestScoreDoc s2) {
      return SUGGEST_SCORE_DOC_COMPARATOR.compare(s1, s2) > 0;
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
