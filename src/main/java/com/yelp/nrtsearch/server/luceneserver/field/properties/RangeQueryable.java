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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import com.yelp.nrtsearch.server.grpc.RangeQuery;
import org.apache.lucene.search.Query;

/**
 * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types that can
 * be queried by a range query.
 */
public interface RangeQueryable {
  /**
   * Build a range query for this field type with the given configuration.
   *
   * @param rangeQuery range query configuration
   * @return lucene range query
   */
  Query getRangeQuery(RangeQuery rangeQuery);
}
