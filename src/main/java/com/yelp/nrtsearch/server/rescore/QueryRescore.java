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
package com.yelp.nrtsearch.server.rescore;

import java.io.IOException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.TopDocs;

/**
 * A implementation of {@link QueryRescorer} that uses a provided Query to assign scores to the
 * first-pass hits. The final score is defined by the combine function and is calculated as follows:
 * <i>final_score = queryWeight * firstPassScore + rescoreQueryWeight * secondPassScore</i>
 */
public final class QueryRescore extends QueryRescorer implements RescoreOperation {

  private final double queryWeight;
  private final double rescoreQueryWeight;

  private QueryRescore(Builder builder) {
    super(builder.query);
    this.queryWeight = builder.queryWeight;
    this.rescoreQueryWeight = builder.rescoreQueryWeight;
  }

  @Override
  protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
    if (!secondPassMatches) {
      return (float) (queryWeight * firstPassScore);
    }
    return (float) (queryWeight * firstPassScore + rescoreQueryWeight * secondPassScore);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException {
    return rescore(
        context.getSearchContext().getSearcherAndTaxonomy().searcher,
        hits,
        context.getWindowSize());
  }

  public static class Builder {

    private Query query;
    private double queryWeight;
    private double rescoreQueryWeight;

    private Builder() {}

    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    public Builder setQueryWeight(double queryWeight) {
      this.queryWeight = queryWeight;
      return this;
    }

    public Builder setRescoreQueryWeight(double rescoreQueryWeight) {
      this.rescoreQueryWeight = rescoreQueryWeight;
      return this;
    }

    public QueryRescore build() {
      return new QueryRescore(this);
    }
  }
}
