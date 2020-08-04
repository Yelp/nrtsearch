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

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;

/** Class for executing the query phase of a search query. */
public class QueryOperation {

  private QueryOperation() {}

  /**
   * Execute the query phase of a search. From the given {@link SearchContext} information, perform
   * the required queries to populate the context {@link SearchResponse.Builder} with the resulting
   * hits. The hits will only contain the lucene doc ids and scores/sort.
   *
   * @param context search context
   * @throws IOException on search error
   */
  public static void execute(SearchContext context) throws IOException {
    long searchStartTime = System.nanoTime();
    TopDocs hits;
    try {
      hits =
          context
              .searcherAndTaxonomy()
              .searcher
              .search(context.query(), context.collector().getManager());
    } catch (TimeLimitingCollector.TimeExceededException tee) {
      context.searchResponse().setHitTimeout(true);
      return;
    }
    context
        .searchResponse()
        .getDiagnosticsBuilder()
        .setFirstPassSearchTimeMs(((System.nanoTime() - searchStartTime) / 1000000.0));

    hits = getHitsFromOffset(hits, context.startHit());
    setResponseHits(context, hits);

    SearchResponse.SearchState.Builder searchState = SearchResponse.SearchState.newBuilder();
    searchState.setTimestamp(context.timestampSec());
    searchState.setSearcherVersion(
        ((DirectoryReader) context.searcherAndTaxonomy().searcher.getIndexReader()).getVersion());
    if (hits.scoreDocs.length != 0) {
      ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length - 1];
      searchState.setLastDocId(lastHit.doc);
      context.collector().fillLastHit(searchState, lastHit);
    }
    context.searchResponse().setSearchState(searchState);
  }

  /**
   * Given all the top documents and a starting offset, produce a slice of the documents starting
   * from that offset.
   *
   * @param hits all hits
   * @param startHit offset into top docs
   * @return slice of hits starting at given offset, or empty slice if there are less startHit docs
   */
  private static TopDocs getHitsFromOffset(TopDocs hits, int startHit) {
    if (startHit != 0) {
      // Slice:
      int count = Math.max(0, hits.scoreDocs.length - startHit);
      ScoreDoc[] newScoreDocs = new ScoreDoc[count];
      if (count > 0) {
        System.arraycopy(hits.scoreDocs, startHit, newScoreDocs, 0, count);
      }
      return new TopDocs(hits.totalHits, newScoreDocs);
    }
    return hits;
  }

  /**
   * Add {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder}s to the context {@link
   * SearchResponse.Builder} for each of the query hits. Populate the builders with the lucene doc
   * id and ranking info.
   *
   * @param context search context
   * @param hits hits from query
   */
  private static void setResponseHits(SearchContext context, TopDocs hits) {
    TotalHits totalHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(hits.totalHits.relation.name()))
            .setValue(hits.totalHits.value)
            .build();
    context.searchResponse().setTotalHits(totalHits);
    for (int hitIndex = 0; hitIndex < hits.scoreDocs.length; hitIndex++) {
      var hitResponse = context.searchResponse().addHitsBuilder();
      ScoreDoc hit = hits.scoreDocs[hitIndex];
      hitResponse.setLuceneDocId(hit.doc);
      context.collector().fillHitRanking(hitResponse, hit);
    }
  }
}
