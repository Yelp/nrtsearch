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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;

public class InnerHitFetchTask implements FetchTask {
  private static final double TEN_TO_THE_POWER_SIX = Math.pow(10, 6);

  public InnerHitContext getInnerHitContext() {
    return innerHitContext;
  }

  private final InnerHitContext innerHitContext;

  private final DoubleAdder getFieldsTimeMs = new DoubleAdder();
  private final DoubleAdder firstPassSearchTimeMs = new DoubleAdder();

  public InnerHitFetchTask(InnerHitContext innerHitContext) {
    this.innerHitContext = innerHitContext;
  }

  public void processHit(
      SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
      throws IOException {
    long startTime = System.nanoTime();

    ParentChildrenBlockJoinQuery parentChildrenBlockJoinQuery =
        new ParentChildrenBlockJoinQuery(
            innerHitContext.getParentFilter(), innerHitContext.getQuery(), hit.getLuceneDocId());

    IndexSearcher searcher = innerHitContext.getSearcherAndTaxonomy().searcher;
    TopDocs topDocs =
        searcher.search(parentChildrenBlockJoinQuery, innerHitContext.getCollector().getManager());
    if (innerHitContext.getStartHit() > 0) {
      topDocs =
          SearchHandler.getHitsFromOffset(
              topDocs, innerHitContext.getStartHit(), innerHitContext.getTopHits());
    }
    firstPassSearchTimeMs.add(((System.nanoTime() - startTime) / TEN_TO_THE_POWER_SIX));

    startTime = System.nanoTime();
    HitsResult.Builder innerHitResultBuilder = HitsResult.newBuilder();
    TotalHits totalInnerHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(topDocs.totalHits.relation.name()))
            .setValue(topDocs.totalHits.value)
            .build();
    innerHitResultBuilder.setTotalHits(totalInnerHits);
    for (int hitIndex = 0; hitIndex < topDocs.scoreDocs.length; hitIndex++) {
      var hitResponse = innerHitResultBuilder.addHitsBuilder();
      ScoreDoc innerHit = topDocs.scoreDocs[hitIndex];
      hitResponse.setLuceneDocId(innerHit.doc);
      hitResponse.setScore(innerHit.score);
    }

    // sort hits by lucene doc id
    List<Hit.Builder> hitBuilders = new ArrayList<>(innerHitResultBuilder.getHitsBuilderList());
    hitBuilders.sort(Comparator.comparing(Hit.Builder::getLuceneDocId));

    new SearchHandler.FillDocsTask(innerHitContext, hitBuilders).run();

    if (hitBuilders.size() > 0) {
      hit.putInnerHits(innerHitContext.getInnerHitName(), innerHitResultBuilder.build());
    }

    getFieldsTimeMs.add(((System.nanoTime() - startTime) / TEN_TO_THE_POWER_SIX));
  }

  public SearchResponse.Diagnostics getDiagnostic() {
    Builder builder =
        Diagnostics.newBuilder()
            .setFirstPassSearchTimeMs(firstPassSearchTimeMs.doubleValue())
            .setGetFieldsTimeMs(getFieldsTimeMs.doubleValue());
    if (innerHitContext.getHighlightFetchTask() != null) {
      builder.setHighlightTimeMs(innerHitContext.getHighlightFetchTask().getTimeTakenMs());
    }
    return builder.build();
  }
}
