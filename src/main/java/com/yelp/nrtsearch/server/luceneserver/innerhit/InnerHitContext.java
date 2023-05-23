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

import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.doc.DefaultSharedDocContext;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightFetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;

public class InnerHitContext extends SearchContext {

  public BitSetProducer getParentFilter() {
    return parentFilter;
  }

  public String getInnerHitName() {
    return innerHitName;
  }

  private String innerHitName;
  private BitSetProducer parentFilter;

  public InnerHitContext(
      String innerHitName,
      SearchContext.Builder parentSearchContextBuilder,
      String queryNestedPath,
      Query innerQuery,
      int startHit,
      int topHits,
      Map<String, FieldDef> retrieveFields,
      DocCollector collectors,
      HighlightFetchTask highlightFetchTask) {
    super(
        newBuilder()
            .setIndexState(parentSearchContextBuilder.getIndexState())
            .setShardState(parentSearchContextBuilder.getShardState())
            .setSearcherAndTaxonomy(parentSearchContextBuilder.getSearcherAndTaxonomy())
            .setTimestampSec(System.currentTimeMillis() / 1000)
            .setStartHit(startHit)
            .setTopHits(topHits)
            .setQueryFields(parentSearchContextBuilder.getQueryFields())
            .setRetrieveFields(retrieveFields)
            .setQuery(innerQuery)
            .setCollector(collectors)
            .setRescorers(Collections.emptyList())
            .setFetchTasks(new FetchTasks(Collections.emptyList()))
            .setHighlightFetchTask(highlightFetchTask)
            .setQueryNestedPath(queryNestedPath)
            .setExtraContext(new HashMap<>())
            .setSharedDocContext(new DefaultSharedDocContext()),
        true);
    this.innerHitName = innerHitName;
    this.parentFilter =
        new QueryBitSetProducer(
            QueryNodeMapper.getInstance()
                .getNestedPathQuery(parentSearchContextBuilder.getQueryNestedPath()));
  }
}
