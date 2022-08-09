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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor.TOTAL_HITS_THRESHOLD;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.search.SortParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collector for getting documents ranked by sorting fields. */
public class SortFieldCollector extends DocCollector {
  private static final Logger logger = LoggerFactory.getLogger(SortFieldCollector.class);

  private final CollectorManager<TopFieldCollector, TopFieldDocs> manager;
  private final Sort sort;
  private final List<String> sortNames;

  public SortFieldCollector(
      CollectorCreatorContext context,
      List<AdditionalCollectorManager<? extends Collector, ? extends CollectorResult>>
          additionalCollectors) {
    super(context, additionalCollectors);
    FieldDoc searchAfter = null;
    int topHits = getNumHitsToCollect();
    int totalHitsThreshold = TOTAL_HITS_THRESHOLD;
    // if there are additional collectors, we cannot skip any recalled docs
    if (!additionalCollectors.isEmpty()) {
      totalHitsThreshold = Integer.MAX_VALUE;
      if (context.getRequest().getTotalHitsThreshold() != 0) {
        logger.warn("Query totalHitsThreshold ignored when using additional collectors");
      }
    } else if (context.getRequest().getTotalHitsThreshold() != 0) {
      totalHitsThreshold = context.getRequest().getTotalHitsThreshold();
    }

    sortNames =
        new ArrayList<>(context.getRequest().getQuerySort().getFields().getSortedFieldsCount());
    try {
      sort =
          SortParser.parseSort(
              context.getRequest().getQuerySort().getFields().getSortedFieldsList(),
              sortNames,
              context.getQueryFields());
    } catch (SearchHandler.SearchHandlerException e) {
      throw new IllegalArgumentException(e);
    }
    manager = TopFieldCollector.createSharedManager(sort, topHits, searchAfter, totalHitsThreshold);
  }

  @Override
  public CollectorManager<? extends Collector, ? extends TopDocs> getManager() {
    return manager;
  }

  @Override
  public void fillHitRanking(SearchResponse.Hit.Builder hitResponse, ScoreDoc scoreDoc) {
    FieldDoc fd = (FieldDoc) scoreDoc;
    if (fd.fields.length != sort.getSort().length) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and ScoreDoc: "
              + sort.getSort().length
              + " != "
              + fd.fields.length);
    }
    if (fd.fields.length != sortNames.size()) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and Sort names: "
              + fd.fields.length
              + " != "
              + sortNames.size());
    }

    for (int i = 0; i < fd.fields.length; ++i) {
      SortField sortField = sort.getSort()[i];
      hitResponse.putSortedFields(
          sortNames.get(i), SortParser.getValueForSortField(sortField, fd.fields[i]));
    }
  }

  @Override
  public void fillLastHit(SearchResponse.SearchState.Builder stateBuilder, ScoreDoc lastHit) {
    FieldDoc fd = (FieldDoc) lastHit;
    for (Object fv : fd.fields) {
      stateBuilder.addLastFieldValues(fv.toString());
    }
  }
}
