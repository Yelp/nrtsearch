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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.HitsResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TopHitsCollector;
import com.yelp.nrtsearch.server.grpc.TotalHits;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks;
import com.yelp.nrtsearch.server.luceneserver.search.FieldFetchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SortParser;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

/** Additional collector that collects top hits based on relevance score or sorting. */
public class TopHitsCollectorManager
    implements AdditionalCollectorManager<
        TopHitsCollectorManager.TopHitsCollector, CollectorResult> {
  private final String name;
  private final com.yelp.nrtsearch.server.grpc.TopHitsCollector grpcTopHitsCollector;
  private final CollectorManager<? extends Collector, ? extends TopDocs> collectorManager;
  private final Sort sort;
  private final List<String> sortNames;
  private final RetrievalContext retrievalContext;
  private SearchContext searchContext;

  /** Implementation of {@link FieldFetchContext} to use for retrieving document fields. */
  class RetrievalContext implements FieldFetchContext {
    private final FetchTasks fetchTasks = new FetchTasks(Collections.emptyList());
    private final Map<String, FieldDef> retrieveFields;

    RetrievalContext(CollectorCreatorContext collectorContext) {
      Map<String, FieldDef> fieldDefMap = new HashMap<>();
      for (String field : grpcTopHitsCollector.getRetrieveFieldsList()) {
        FieldDef fieldDef = collectorContext.getQueryFields().get(field);
        if (fieldDef == null) {
          throw new IllegalStateException("Unknown field: " + field);
        }
        fieldDefMap.put(field, fieldDef);
      }
      retrieveFields = Collections.unmodifiableMap(fieldDefMap);
    }

    @Override
    public SearcherAndTaxonomy getSearcherAndTaxonomy() {
      return searchContext.getSearcherAndTaxonomy();
    }

    @Override
    public Map<String, FieldDef> getRetrieveFields() {
      return retrieveFields;
    }

    @Override
    public FetchTasks getFetchTasks() {
      return fetchTasks;
    }

    @Override
    public SearchContext getSearchContext() {
      return searchContext;
    }
  }

  /**
   * Constructor.
   *
   * @param name collector name
   * @param grpcTopHitsCollector message definition
   * @param context collector creation context
   */
  public TopHitsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TopHitsCollector grpcTopHitsCollector,
      CollectorCreatorContext context) {
    this.name = name;
    this.grpcTopHitsCollector = grpcTopHitsCollector;
    this.retrievalContext = new RetrievalContext(context);

    if (grpcTopHitsCollector.hasQuerySort()) {
      sortNames =
          new ArrayList<>(grpcTopHitsCollector.getQuerySort().getFields().getSortedFieldsCount());
      try {
        sort =
            SortParser.parseSort(
                grpcTopHitsCollector.getQuerySort().getFields().getSortedFieldsList(),
                sortNames,
                context.getQueryFields());
      } catch (SearchHandler.SearchHandlerException e) {
        throw new IllegalArgumentException(e);
      }
      collectorManager =
          TopFieldCollector.createSharedManager(
              sort, grpcTopHitsCollector.getTopHits(), null, Integer.MAX_VALUE);
    } else {
      collectorManager =
          TopScoreDocCollector.createSharedManager(
              grpcTopHitsCollector.getTopHits(), null, Integer.MAX_VALUE);
      sort = null;
      sortNames = null;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setSearchContext(SearchContext searchContext) {
    this.searchContext = searchContext;
    // Backward compatibility: We reuse the highlightFetchTask from the SearchContext
    this.retrievalContext.fetchTasks.setHighlightFetchTask(
        searchContext.getFetchTasks().getHighlightFetchTask());
  }

  @Override
  public TopHitsCollector newCollector() throws IOException {
    return new TopHitsCollector(collectorManager.newCollector());
  }

  @Override
  public CollectorResult reduce(Collection<TopHitsCollector> collectors) throws IOException {
    Collection<Collector> hitsCollectors =
        collectors.stream().map(c -> c.collector).collect(Collectors.toList());
    @SuppressWarnings("unchecked")
    TopDocs topDocs =
        ((CollectorManager<Collector, ? extends TopDocs>) collectorManager).reduce(hitsCollectors);
    topDocs =
        SearchHandler.getHitsFromOffset(
            topDocs, grpcTopHitsCollector.getStartHit(), grpcTopHitsCollector.getTopHits());

    HitsResult.Builder hitsResultBuilder = HitsResult.newBuilder();

    TotalHits totalHits =
        TotalHits.newBuilder()
            .setRelation(TotalHits.Relation.valueOf(topDocs.totalHits.relation.name()))
            .setValue(topDocs.totalHits.value)
            .build();
    hitsResultBuilder.setTotalHits(totalHits);
    for (int hitIndex = 0; hitIndex < topDocs.scoreDocs.length; hitIndex++) {
      var hitResponse = hitsResultBuilder.addHitsBuilder();
      ScoreDoc hit = topDocs.scoreDocs[hitIndex];
      hitResponse.setLuceneDocId(hit.doc);
      if (grpcTopHitsCollector.hasQuerySort()) {
        FieldDoc fd = (FieldDoc) hit;
        for (int i = 0; i < fd.fields.length; ++i) {
          SortField sortField = sort.getSort()[i];
          hitResponse.putSortedFields(
              sortNames.get(i), SortParser.getValueForSortField(sortField, fd.fields[i]));
        }
        hitResponse.setScore(Double.NaN);
      } else {
        hitResponse.setScore(hit.score);
      }
    }
    // sort hits by lucene doc id
    List<Hit.Builder> hitBuilders = new ArrayList<>(hitsResultBuilder.getHitsBuilderList());
    hitBuilders.sort(Comparator.comparing(Hit.Builder::getLuceneDocId));

    new SearchHandler.FillDocsTask(retrievalContext, hitBuilders).run();

    if (grpcTopHitsCollector.getExplain()) {
      hitBuilders.forEach(
          hitBuilder -> {
            try {
              hitBuilder.setExplain(
                  searchContext
                      .getSearcherAndTaxonomy()
                      .searcher
                      .explain(searchContext.getQuery(), hitBuilder.getLuceneDocId())
                      .toString());
            } catch (IOException ioException) {
              // ignore failed explain
            }
          });
    }
    return CollectorResult.newBuilder().setHitsResult(hitsResultBuilder.build()).build();
  }

  /** Collector to wrap document collector. */
  public static class TopHitsCollector implements Collector {
    final Collector collector;

    TopHitsCollector(Collector collector) {
      this.collector = collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new TopHitsLeafCollector(collector.getLeafCollector(context));
    }

    @Override
    public ScoreMode scoreMode() {
      return collector.scoreMode();
    }
  }

  /** Leaf collector to wrap document leaf collector. */
  public static class TopHitsLeafCollector implements LeafCollector {
    final LeafCollector leafCollector;

    TopHitsLeafCollector(LeafCollector leafCollector) {
      this.leafCollector = leafCollector;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      leafCollector.collect(doc);
    }
  }
}
