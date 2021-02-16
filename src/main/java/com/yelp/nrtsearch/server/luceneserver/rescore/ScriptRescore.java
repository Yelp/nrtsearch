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

import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.QueryRescorer;

public class ScriptRescore extends QueryRescorer {

  public ScriptRescore(DoubleValuesSource doubleValuesSource) {
    super(new FunctionScoreQuery(new MatchAllDocsQuery(), doubleValuesSource));
  }

  @Override
  protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
    return secondPassScore;
  }
}
