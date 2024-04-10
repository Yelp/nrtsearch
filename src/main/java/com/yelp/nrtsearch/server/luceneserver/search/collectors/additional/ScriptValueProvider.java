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

import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Scorable;

/** Value provider that uses a {@link ScoreScript}. */
class ScriptValueProvider implements ValueProvider {
  final DoubleValuesSource valuesSource;
  final double unsetVal;

  /**
   * Constructor.
   *
   * @param script script definition
   * @param lookup doc value lookup
   */
  ScriptValueProvider(Script script, DocLookup lookup, double unsetValue) {
    ScoreScript.Factory factory = ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
    valuesSource =
        factory.newFactory(ScriptParamsUtils.decodeParams(script.getParamsMap()), lookup);
    unsetVal = unsetValue;
  }

  @Override
  public LeafValueProvider getLeafValueProvider(LeafReaderContext leafContext) throws IOException {
    return new ScriptLeafValueProvider(leafContext);
  }

  @Override
  public boolean needsScore() {
    return valuesSource.needsScores();
  }

  /** Leaf value provider that uses a {@link ScoreScript}. */
  class ScriptLeafValueProvider implements LeafValueProvider {
    final DoubleValues values;
    final QueryUtils.ScorableDoubleValues scores;

    /**
     * Constructor.
     *
     * @param leafContext segment context
     * @throws IOException
     */
    ScriptLeafValueProvider(LeafReaderContext leafContext) throws IOException {
      scores = new QueryUtils.ScorableDoubleValues();
      values = valuesSource.getValues(leafContext, scores);
    }

    @Override
    public void setScorer(Scorable scorer) {
      scores.setScorer(scorer);
    }

    @Override
    public double getValue(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        return values.doubleValue();
      } else {
        return unsetVal;
      }
    }
  }
}
