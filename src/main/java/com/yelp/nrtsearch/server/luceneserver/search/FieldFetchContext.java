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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;

/**
 * Interface to provided needed information to use {@link
 * com.yelp.nrtsearch.server.luceneserver.SearchHandler.FillFieldsTask} to retrieve document fields.
 */
public interface FieldFetchContext {

  /** Get searcher. */
  SearcherAndTaxonomy getSearcherAndTaxonomy();

  /** Get fields to retrieve. */
  Map<String, FieldDef> getRetrieveFields();

  /** Get fetch tasks */
  FetchTasks getFetchTasks();

  /** Get search context. Only used for executing fetch tasks. */
  SearchContext getSearchContext();
}
