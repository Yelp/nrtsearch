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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SortType;

/**
 * * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types,
 * denoting that this field's sorted value shall be processed before return. We implicitly assume
 * any class implement this interface is also {@link Sortable}.
 */
public interface HasSortableValueParser {

  /**
   * *
   *
   * @param querySortField the sort settings in query
   * @param value the original sorted value
   * @return the parsed sorted value in wrapped in {@link CompositeFieldValue}
   */
  CompositeFieldValue parseSortedValue(SortType querySortField, Object value);
}
