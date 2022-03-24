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

import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

/**
 * Extension of lucene {@link CollectorManager} interface. Allows us to add any operations that
 * might be useful, such as aggregations.
 *
 * @param <C> collector type
 * @param <T> parallel collection reduction type
 */
public interface AdditionalCollectorManager<C extends Collector, T> extends CollectorManager<C, T> {
  /** Get collection name as it should appear in the search response. */
  String getName();

  /** Sets the search context. This must be called before collecting */
  default void setSearchContext(SearchContext searchContext) {}
}
