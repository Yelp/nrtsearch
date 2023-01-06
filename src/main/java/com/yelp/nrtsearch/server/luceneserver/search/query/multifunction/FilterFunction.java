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

import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

/**
 * Function base class used by {@link
 * com.yelp.nrtsearch.server.luceneserver.search.query.multifunction.MultiFunctionScoreQuery}. Can
 * optionally have a filter {@link Query} to apply this function only to those matching docs.
 * Functions may also specify a weight to scale the result.
 */
public abstract class FilterFunction {
  private final Query filterQuery;
  private final float weight;

  /** Interface for function implementation at the index leaf level. */
  public interface LeafFunction {

    /**
     * Produce the function score for a document id in this leaf, should have weight applied.
     *
     * @param docId segment level doc id
     * @param innerQueryScore the document score value for the main MultiFunctionScoreQuery query
     * @return weighted function score value
     * @throws IOException
     */
    double score(int docId, float innerQueryScore) throws IOException;

    /**
     * Produce the score explanation for a document id in this leaf.
     *
     * @param docId segment level doc id
     * @param innerQueryScore the document score value for the main MultiFunctionScoreQuery query
     * @return weighted function score explanation
     * @throws IOException
     */
    Explanation explainScore(int docId, Explanation innerQueryScore) throws IOException;
  }

  /**
   * Builder method to create a {@link FilterFunction} object based on its gRPC message definition.
   *
   * @param filterFunctionGrpc function gRPC definition
   * @param indexState index state
   * @return filter function object
   */
  public static FilterFunction build(
      MultiFunctionScoreQuery.FilterFunction filterFunctionGrpc, IndexState indexState) {
    Query filterQuery =
        filterFunctionGrpc.hasFilter()
            ? QueryNodeMapper.getInstance().getQuery(filterFunctionGrpc.getFilter(), indexState)
            : null;
    float weight = filterFunctionGrpc.getWeight() != 0.0f ? filterFunctionGrpc.getWeight() : 1.0f;
    switch (filterFunctionGrpc.getFunctionCase()) {
      case SCRIPT:
        ScoreScript.Factory factory =
            ScriptService.getInstance()
                .compile(filterFunctionGrpc.getScript(), ScoreScript.CONTEXT);
        DoubleValuesSource scriptSource =
            factory.newFactory(
                ScriptParamsUtils.decodeParams(filterFunctionGrpc.getScript().getParamsMap()),
                indexState.docLookup);
        return new ScriptFilterFunction(
            filterQuery, weight, filterFunctionGrpc.getScript(), scriptSource);
      case FUNCTION_NOT_SET:
        return new WeightFilterFunction(filterQuery, weight);
      default:
        throw new IllegalArgumentException(
            "Unknown function type: " + filterFunctionGrpc.getFunctionCase());
    }
  }

  /**
   * Constructor.
   *
   * @param filterQuery filter to use when applying this function, or null if none
   * @param weight weight multiple to scale the function score
   */
  public FilterFunction(Query filterQuery, float weight) {
    this.filterQuery = filterQuery;
    this.weight = weight;
  }

  /** If this function uses a filter query. */
  public boolean hasFilterQuery() {
    return filterQuery != null;
  }

  /** Get the function filter query, or null if none */
  public Query getFilterQuery() {
    return filterQuery;
  }

  /** Get the function weight. */
  public float getWeight() {
    return weight;
  }

  /**
   * Get the function implementation as applies to an index leaf segment.
   *
   * @param leafContext segment context
   * @return leaf function implementation
   * @throws IOException
   */
  public abstract LeafFunction getLeafFunction(LeafReaderContext leafContext) throws IOException;

  /**
   * Method to rewrite queries with the given {@link IndexReader}. Final to force use of {@link
   * #doRewrite(IndexReader, boolean, Query)}.
   *
   * @param reader index reader
   * @return function object with any query rewriting done
   * @throws IOException
   */
  public final FilterFunction rewrite(IndexReader reader) throws IOException {
    Query rewrittenFilterQuery = null;
    if (filterQuery != null) {
      rewrittenFilterQuery = filterQuery.rewrite(reader);
    }
    return doRewrite(reader, rewrittenFilterQuery != filterQuery, rewrittenFilterQuery);
  }

  /**
   * Rewrite method for all child classes.
   *
   * @param reader index reader
   * @param filterQueryRewritten if the filter query was modified by rewrite
   * @param rewrittenFilterQuery final value of rewritten query, may be null if no filter
   * @return fully rewritten filter function
   * @throws IOException
   */
  protected abstract FilterFunction doRewrite(
      IndexReader reader, boolean filterQueryRewritten, Query rewrittenFilterQuery)
      throws IOException;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (hasFilterQuery()) {
      sb.append("filter(").append(getFilterQuery()).append("), ");
    }
    sb.append("weight:").append(getWeight());
    return sb.toString();
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    FilterFunction other = (FilterFunction) obj;
    return Objects.equals(weight, other.weight)
        && Objects.equals(filterQuery, ((FilterFunction) obj).filterQuery)
        && doEquals(other);
  }

  /**
   * Equals method for all child classes. Should check everything but the filter query and weight.
   */
  protected abstract boolean doEquals(FilterFunction other);

  @Override
  public final int hashCode() {
    return Objects.hash(weight, filterQuery, doHashCode());
  }

  /**
   * Hash code method for all child classes. Should hash everything but the filter query and weight.
   */
  protected abstract int doHashCode();
}
