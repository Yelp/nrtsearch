/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.query.vector;

import org.apache.lucene.search.TotalHits;

/**
 * Interface for vector queries to extract the knn {@link TotalHits}. This contains the total number
 * of vector comparisons performed during the query and is useful for debugging and perf analysis.
 * The rewritten query does not expose this information.
 */
public interface WithVectorTotalHits {
  /**
   * Get the total hits from the vector query. This may be null if the query has not yet been
   * rewritten.
   *
   * @return total hits
   */
  TotalHits getTotalHits();
}
