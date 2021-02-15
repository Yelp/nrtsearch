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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import org.apache.lucene.search.Query;

public final class QueryRescore extends org.apache.lucene.search.QueryRescorer {

  private double queryWeight;
  private double secondQueryWeight;

  public QueryRescore(Query rescoreQuery, double queryWeight, double secondQueryWeight) {
    super(rescoreQuery);
    this.queryWeight = queryWeight;
    this.secondQueryWeight = secondQueryWeight;
  }

  @Override
  protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
    if (!secondPassMatches) {
      return firstPassScore;
    }
    return (float) (queryWeight * firstPassScore + secondQueryWeight * secondPassScore);
  }
}
