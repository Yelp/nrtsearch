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
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.DocCollector;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.search.Query;

/** Search context interface to provide all the information to perform a search. */
public interface SearchContext {
  /** Get query index state. */
  IndexState indexState();

  /** Get query shard state. */
  ShardState shardState();

  /** Get searcher instance for query. */
  SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy();

  /** Get container to store diagnostic information in response. */
  SearchResponse.Diagnostics.Builder diagnostics();

  /** Get timestamp to use for query. */
  long timestampSec();

  /** Get the offset of the first hit to return from the top hits. */
  int startHit();

  /** Get map of all fields needed for this query. */
  Map<String, FieldDef> queryFields();

  /** Get all field names that should be returned as fields. */
  Set<String> retrieveFieldNames();

  /** Get all sort field names that should be returned as sortedFields. */
  Set<String> sortFieldNames();

  /** Get final lucene query to perform. */
  Query query();

  /** Get collector to manage query hits. */
  DocCollector collector();
}
