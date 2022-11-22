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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils.SettableDoubleValues;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

/**
 * Filter function implementation that produces document values with a provided {@link
 * com.yelp.nrtsearch.server.luceneserver.script.ScoreScript} definition.
 */
public class ScriptFilterFunction extends FilterFunction {
  private static final JsonFormat.Printer SCRIPT_PRINTER =
      JsonFormat.printer().omittingInsignificantWhitespace();
  private final Script script;
  private final DoubleValuesSource scriptSource;

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   * @param script script definition
   * @param scriptSource compiled script
   */
  public ScriptFilterFunction(
      Query filterQuery, float weight, Script script, DoubleValuesSource scriptSource) {
    super(filterQuery, weight);
    this.script = script;
    this.scriptSource = scriptSource;
  }

  @Override
  protected FilterFunction doRewrite(
      IndexReader reader, boolean filterQueryRewritten, Query rewrittenFilterQuery)
      throws IOException {
    if (filterQueryRewritten) {
      return new ScriptFilterFunction(rewrittenFilterQuery, getWeight(), script, scriptSource);
    } else {
      return this;
    }
  }

  @Override
  protected boolean doEquals(FilterFunction other) {
    if (other == null) {
      return false;
    }
    if (other.getClass() != this.getClass()) {
      return false;
    }
    ScriptFilterFunction otherScriptFilter = (ScriptFilterFunction) other;
    return Objects.equals(script, otherScriptFilter.script);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(script);
  }

  @Override
  public LeafFunction getLeafFunction(LeafReaderContext leafContext) throws IOException {
    return new ScriptLeafFunction(leafContext);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append(", script:");
    try {
      sb.append(SCRIPT_PRINTER.print(script));
    } catch (InvalidProtocolBufferException e) {
      sb.append("error");
    }
    return sb.toString();
  }

  public final class ScriptLeafFunction implements LeafFunction {
    private final DoubleValues leafValues;
    private final SettableDoubleValues innerScoreValues = new SettableDoubleValues();

    public ScriptLeafFunction(LeafReaderContext context) throws IOException {
      this.leafValues = scriptSource.getValues(context, innerScoreValues);
    }

    @Override
    public double score(int docId, float innerQueryScore) throws IOException {
      return scoreScript(docId, innerQueryScore) * getWeight();
    }

    private double scoreScript(int docId, float innerQueryScore) throws IOException {
      leafValues.advanceExact(docId);
      innerScoreValues.setValue(innerQueryScore);
      return leafValues.doubleValue();
    }

    @Override
    public Explanation explainScore(int docId, Explanation innerQueryScore) throws IOException {
      double score = scoreScript(docId, innerQueryScore.getValue().floatValue());
      String explanation = "script score function, computed with script:\"" + script + "\"";
      Explanation scoreExp =
          Explanation.match(innerQueryScore.getValue(), "_score: ", innerQueryScore);
      Explanation scriptExpl = Explanation.match((float) score, explanation, scoreExp);
      return Explanation.match(
          scriptExpl.getValue().floatValue() * getWeight(),
          "product of:",
          scriptExpl,
          Explanation.match(getWeight(), "weight"));
    }
  }
}
