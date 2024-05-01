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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import java.io.IOException;
import org.apache.lucene.search.Scorable;

/** Interface for providing a double value for a segment document. */
public interface LeafValueProvider {
  /**
   * Set scorer for current doc id.
   *
   * @param scorer scorer
   * @throws IOException
   */
  void setScorer(Scorable scorer) throws IOException;

  /**
   * Get a double value for the given doc id.
   *
   * @param doc doc id
   * @throws IOException
   */
  double getValue(int doc) throws IOException;
}
