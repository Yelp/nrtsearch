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
package com.yelp.nrtsearch.server.query;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.search.FieldFetchContext;
import com.yelp.nrtsearch.server.search.SearchContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetch task that retrieves fields from secondary index documents matched by a {@link
 * com.yelp.nrtsearch.server.grpc.CrossIndexQuery}. Reuses the same searcher snapshot that was used
 * for filtering, ensuring consistency between the join filter and the retrieved fields.
 *
 * <p>Implements {@link AutoCloseable} to release the secondary searcher after the fetch phase.
 */
public class CrossIndexHitFetchTask implements FetchTask, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CrossIndexHitFetchTask.class);
  private static final int DEFAULT_TOP_HITS = 3;

  private final String indexName;
  private final String primaryField;
  private final String secondaryField;
  private final Query innerQuery;
  private final SearcherAndTaxonomy secondarySearcher;
  private final ShardState secondaryShard;
  private final Map<String, FieldDef> retrieveFields;
  private final int topHits;

  /**
   * @param indexName name of the secondary index (used as inner_hits key)
   * @param primaryField field name in the primary index containing the join key
   * @param secondaryField field name in the secondary index containing the join key
   * @param innerQuery the query executed against the secondary index
   * @param secondarySearcher the searcher snapshot (same one used for the join filter)
   * @param secondaryShard shard state for releasing the searcher
   * @param retrieveFields fields to fetch from secondary hits
   * @param topHits max secondary hits per primary hit (0 means default)
   */
  public CrossIndexHitFetchTask(
      String indexName,
      String primaryField,
      String secondaryField,
      Query innerQuery,
      SearcherAndTaxonomy secondarySearcher,
      ShardState secondaryShard,
      Map<String, FieldDef> retrieveFields,
      int topHits) {
    this.indexName = indexName;
    this.primaryField = primaryField;
    this.secondaryField = secondaryField;
    this.innerQuery = innerQuery;
    this.secondarySearcher = secondarySearcher;
    this.secondaryShard = secondaryShard;
    this.retrieveFields = retrieveFields;
    this.topHits = topHits > 0 ? topHits : DEFAULT_TOP_HITS;
  }

  @Override
  public void processHit(
      SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
      throws IOException {
    // Read the join key directly from doc values (doesn't require primary_field in retrieveFields)
    String joinKeyValue = getJoinKeyFromDocValues(hitLeaf, hit.getLuceneDocId());
    if (joinKeyValue == null) {
      return;
    }

    // Query secondary index: innerQuery AND secondaryField = joinKeyValue
    BooleanQuery secondaryQuery =
        new BooleanQuery.Builder()
            .add(innerQuery, BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(secondaryField, joinKeyValue)), BooleanClause.Occur.FILTER)
            .build();

    IndexSearcher searcher = secondarySearcher.searcher();
    TopDocs topDocs =
        searcher.search(
            secondaryQuery, new TopScoreDocCollectorManager(topHits, Integer.MAX_VALUE));

    HitsResult.Builder hitsResultBuilder = HitsResult.newBuilder();
    hitsResultBuilder.setTotalHits(
        TotalHits.newBuilder()
            .setValue(topDocs.totalHits.value())
            .setRelation(TotalHits.Relation.valueOf(topDocs.totalHits.relation().name())));

    if (topDocs.scoreDocs.length > 0) {
      // Build hit builders and populate with doc ids + scores
      List<SearchResponse.Hit.Builder> secondaryHitBuilders = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        SearchResponse.Hit.Builder secondaryHit = hitsResultBuilder.addHitsBuilder();
        secondaryHit.setLuceneDocId(scoreDoc.doc);
        secondaryHit.setScore(scoreDoc.score);
        secondaryHitBuilders.add(secondaryHit);
      }

      // Sort by doc id for efficient segment-based field fetching
      secondaryHitBuilders.sort(
          Comparator.comparingInt(SearchResponse.Hit.Builder::getLuceneDocId));

      // Fetch fields using FillDocsTask with a lightweight FieldFetchContext
      new SearchHandler.FillDocsTask(
              new SecondaryFieldFetchContext(secondarySearcher, retrieveFields),
              secondaryHitBuilders)
          .run();
    }

    hit.putInnerHits("cross:" + indexName, hitsResultBuilder.build());
  }

  /**
   * Read the join key value from the primary index's doc values for the given hit.
   *
   * @return the join key string, or null if not found
   */
  private String getJoinKeyFromDocValues(LeafReaderContext leaf, int docId) throws IOException {
    int segmentDocId = docId - leaf.docBase;

    // Try SortedDocValues first (single-valued field)
    SortedDocValues sorted = leaf.reader().getSortedDocValues(primaryField);
    if (sorted != null && sorted.advanceExact(segmentDocId)) {
      return sorted.lookupOrd(sorted.ordValue()).utf8ToString();
    }

    // Fall back to SortedSetDocValues (multi-valued field, take first value)
    SortedSetDocValues sortedSet = leaf.reader().getSortedSetDocValues(primaryField);
    if (sortedSet != null && sortedSet.advanceExact(segmentDocId)) {
      if (sortedSet.docValueCount() > 0) {
        return sortedSet.lookupOrd(sortedSet.nextOrd()).utf8ToString();
      }
    }

    return null;
  }

  @Override
  public void close() {
    try {
      secondaryShard.release(secondarySearcher);
    } catch (Exception e) {
      logger.error("CrossIndexHitFetchTask: failed to release secondary searcher", e);
    }
  }

  /**
   * Lightweight {@link FieldFetchContext} for fetching fields from the secondary index. Does not
   * support explain or nested fetch tasks.
   */
  private record SecondaryFieldFetchContext(
      SearcherAndTaxonomy searcherAndTaxonomy, Map<String, FieldDef> retrieveFields)
      implements FieldFetchContext {

    @Override
    public SearcherAndTaxonomy getSearcherAndTaxonomy() {
      return searcherAndTaxonomy;
    }

    @Override
    public Map<String, FieldDef> getRetrieveFields() {
      return retrieveFields;
    }

    @Override
    public FetchTasks getFetchTasks() {
      return new FetchTasks(List.of());
    }

    @Override
    public SearchContext getSearchContext() {
      return null;
    }

    @Override
    public boolean isExplain() {
      return false;
    }
  }
}
