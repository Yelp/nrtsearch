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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.FuzzyParams;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

public class MatchQueryBuilder extends QueryBuilder {

  int maxEdits;
  int prefixLength;
  int maxExpansions;
  boolean transpositions;

  public MatchQueryBuilder(Analyzer analyzer, FuzzyParams fuzzyParams) {
    super(analyzer);
    this.maxEdits = fuzzyParams.getMaxEdits();
    this.prefixLength = fuzzyParams.getPrefixLength();
    this.maxExpansions = fuzzyParams.getMaxExpansions();
    this.transpositions = fuzzyParams.getTranspositions();
  }

  @Override
  protected Query newTermQuery(Term term) {
    if (maxEdits == 0) {
      return super.newTermQuery(term);
    } else {
      return new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
    }
  }
}
