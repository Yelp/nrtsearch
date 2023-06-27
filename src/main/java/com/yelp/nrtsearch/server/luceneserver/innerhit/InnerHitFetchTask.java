/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.innerhit;

import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics.Builder;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SortParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;

/**
 * InnerHit fetch task does a mini-scale search per hit against all child documents for this hit.
 * Parallelism is at the fetchTask level.
 */
public class InnerHitFetchTask implements FetchTask {
  private static final double NS_PER_MS = Math.pow(10, 6);

  public InnerHitContext getInnerHitContext() {
    return innerHitContext;
  }

  private final InnerHitContext innerHitContext;
  private final IndexSearcher searcher;
  private final BitSetProducer parentFilter;
  private final Weight innerHitWeight;

  private final DoubleAdder getFieldsTimeMs = new DoubleAdder();
  private final DoubleAdder firstPassSearchTimeMs = new DoubleAdder();

  public InnerHitFetchTask(InnerHitContext innerHitContext) throws IOException {
    this.innerHitContext = innerHitContext;
    this.searcher = innerHitContext.getSearcherAndTaxonomy().searcher;
    boolean needScore =
        innerHitContext.getTopHits() >= innerHitContext.getStartHit()
            && (innerHitContext.getSort() == null || innerHitContext.getSort().needsScores());
    // We support TopDocsCollector only, so top_scores is good enough
    this.innerHitWeight =
        searcher
            .rewrite(innerHitContext.getQuery())
            .createWeight(
                searcher, needScore ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE_NO_SCORES, 1f);
    this.parentFilter =
        new QueryBitSetProducer(searcher.rewrite(innerHitContext.getParentFilterQuery()));
  }

  /**
   * Collect all inner hits for each parent hit. Normally, {@link IndexSearcher} will create weight
   * each time we search. But for innerHit, child query weight is reusable. Therefore, for max
   * efficiency, we will not use search from the {@link IndexSearcher}. Instead, we create two
   * weights separately - the non-reusable {@link ParentChildrenBlockJoinQuery}'s weight and the
   * reusable innerHitQuery weight, and then intersect the {@link DocIdSetIterator}s created from
   * them.
   */
  public void processHit(
      SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
      throws IOException {
    long startTime = System.nanoTime();

    // This is just a children selection query for each parent hit. And score is not needed for this
    // filter query.
    ParentChildrenBlockJoinQuery parentChildrenBlockJoinQuery =
        new ParentChildrenBlockJoinQuery(
            parentFilter, innerHitContext.getChildFilterQuery(), hit.getLuceneDocId());
    Weight filterWeight =
        parentChildrenBlockJoinQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    // All child documents are guaranteed to be stored in the same leaf as the parent document.
    // Therefore, a single collector without reduce is sufficient to collect all.
    TopDocsCollector topDocsCollector = innerHitContext.getTopDocsCollectorManager().newCollector();

    intersectWeights(filterWeight, innerHitWeight, topDocsCollector, hitLeaf);
    TopDocs topDocs = topDocsCollector.topDocs();

    if (innerHitContext.getStartHit() > 0) {
      topDocs =
          SearchHandler.getHitsFromOffset(
              topDocs, innerHitContext.getStartHit(), innerHitContext.getTopHits());
    }
    firstPassSearchTimeMs.add(((System.nanoTime() - startTime) / NS_PER_MS));

    startTime = System.nanoTime();
    HitsResult.Builder innerHitResultBuilder = HitsResult.newBuilder();
    TotalHits totalInnerHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(topDocs.totalHits.relation.name()))
            .setValue(topDocs.totalHits.value)
            .build();
    innerHitResultBuilder.setTotalHits(totalInnerHits);

    for (int innerHitIndex = 0; innerHitIndex < topDocs.scoreDocs.length; innerHitIndex++) {
      SearchResponse.Hit.Builder innerHitResponse = innerHitResultBuilder.addHitsBuilder();
      ScoreDoc innerHit = topDocs.scoreDocs[innerHitIndex];
      innerHitResponse.setLuceneDocId(innerHit.doc);
      if (!innerHitContext.getSortedFieldNames().isEmpty()) {
        // fill the sortedFields
        FieldDoc fd = (FieldDoc) innerHit;
        for (int i = 0; i < fd.fields.length; ++i) {
          SortField sortField = innerHitContext.getSort().getSort()[i];
          innerHitResponse.putSortedFields(
              innerHitContext.getSortedFieldNames().get(i),
              SortParser.getValueForSortField(sortField, fd.fields[i]));
        }
        innerHitResponse.setScore(Double.NaN);
      } else {
        innerHitResponse.setScore(innerHit.score);
      }
    }

    // sort hits by lucene doc id
    List<Hit.Builder> hitBuilders = new ArrayList<>(innerHitResultBuilder.getHitsBuilderList());
    hitBuilders.sort(Comparator.comparingInt(Hit.Builder::getLuceneDocId));
    new SearchHandler.FillDocsTask(innerHitContext, hitBuilders).run();

    hit.putInnerHits(innerHitContext.getInnerHitName(), innerHitResultBuilder.build());

    getFieldsTimeMs.add(((System.nanoTime() - startTime) / NS_PER_MS));
  }

  private void intersectWeights(
      Weight filterWeight,
      Weight innerHitWeight,
      TopDocsCollector topDocsCollector,
      LeafReaderContext hitLeaf)
      throws IOException {
    ScorerSupplier filterScorerSupplier = filterWeight.scorerSupplier(hitLeaf);
    if (filterScorerSupplier == null) {
      return;
    }
    Scorer filterScorer = filterScorerSupplier.get(0);

    ScorerSupplier innerHitScorerSupplier = innerHitWeight.scorerSupplier(hitLeaf);
    if (innerHitScorerSupplier == null) {
      return;
    }
    Scorer innerHitScorer = innerHitScorerSupplier.get(0);

    DocIdSetIterator iterator =
        ConjunctionDISI.intersectIterators(
            Arrays.asList(filterScorer.iterator(), innerHitScorer.iterator()));

    LeafCollector leafCollector = topDocsCollector.getLeafCollector(hitLeaf);
    // filterWeight is always COMPLETE_NO_SCORES
    try {
      leafCollector.setScorer(innerHitScorer);
    } catch (CollectionTerminatedException exception) {
      // Same as the indexSearcher, innerHit shall swallow this exception. No doc to collect in this
      // case.
      return;
    }

    int docId;
    while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      leafCollector.collect(docId);
    }
  }

  public SearchResponse.Diagnostics getDiagnostic() {
    Builder builder =
        Diagnostics.newBuilder()
            .setFirstPassSearchTimeMs(firstPassSearchTimeMs.doubleValue())
            .setGetFieldsTimeMs(getFieldsTimeMs.doubleValue());
    if (innerHitContext.getFetchTasks().getHighlightFetchTask() != null) {
      builder.setHighlightTimeMs(
          innerHitContext.getFetchTasks().getHighlightFetchTask().getTimeTakenMs());
    }
    return builder.build();
  }
}
