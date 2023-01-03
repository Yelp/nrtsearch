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
package com.yelp.nrtsearch.server.luceneserver.search.query.multifunction;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

/** Filter function that only adds a constant weight. */
public class WeightFilterFunction extends FilterFunction {
  private final WeightLeafFunction leafFunction;

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   */
  public WeightFilterFunction(Query filterQuery, float weight) {
    super(filterQuery, weight);
    leafFunction = new WeightLeafFunction();
  }

  @Override
  protected FilterFunction doRewrite(
      IndexReader reader, boolean filterQueryRewritten, Query rewrittenFilterQuery)
      throws IOException {
    if (filterQueryRewritten) {
      return new WeightFilterFunction(rewrittenFilterQuery, getWeight());
    } else {
      return this;
    }
  }

  @Override
  protected boolean doEquals(FilterFunction other) {
    if (other == null) {
      return false;
    }
    return other.getClass() == this.getClass();
  }

  @Override
  protected int doHashCode() {
    // weight has already been hashed in base class
    return 0;
  }

  @Override
  public LeafFunction getLeafFunction(LeafReaderContext leafContext) throws IOException {
    return leafFunction;
  }

  public final class WeightLeafFunction implements LeafFunction {

    @Override
    public double score(int docId, float innerQueryScore) throws IOException {
      return getWeight();
    }

    @Override
    public Explanation explainScore(int docId, Explanation innerQueryScore) throws IOException {
      Explanation functionExplanation =
          Explanation.match(1.0f, "constant score 1.0 - no function provided");
      return Explanation.match(
          functionExplanation.getValue().floatValue() * getWeight(),
          "product of:",
          functionExplanation,
          Explanation.match(getWeight(), "weight"));
    }
  }
}
