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
package com.yelp.nrtsearch.server.luceneserver.script;

import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.SegmentDocLookup;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

/**
 * Script to produce a double value for a given document. Implementations must have an execute
 * function. This class conforms with the script compile contract, see {@link ScriptContext}. The
 * script has access to the query parameters, the document doc values through {@link
 * SegmentDocLookup}, and the document score through get_score.
 */
public abstract class ScoreScript extends DoubleValues {
  private static final int DOC_UNSET = -1;

  private final Map<String, Object> params;
  private final SegmentDocLookup segmentDocLookup;
  private final DoubleValues scores;
  private int docId = DOC_UNSET;
  private int scoreDocId = DOC_UNSET;

  // names for parameters to execute
  public static final String[] PARAMETERS = new String[] {};

  /**
   * ScoreScript constructor.
   *
   * @param params script parameters from {@link com.yelp.nrtsearch.server.grpc.Script}
   * @param docLookup index level doc values lookup
   * @param leafContext lucene segment context
   * @param scores provider of segment document scores
   */
  public ScoreScript(
      Map<String, Object> params,
      DocLookup docLookup,
      LeafReaderContext leafContext,
      DoubleValues scores) {
    this.params = params;
    this.segmentDocLookup = docLookup.getSegmentLookup(leafContext);
    this.scores = scores;
  }

  /**
   * Main script function.
   *
   * @return double value computed for document
   */
  public abstract double execute();

  /**
   * Redirect {@link DoubleValues} interface to get script execution result.
   *
   * @return script execution result
   */
  @Override
  public double doubleValue() {
    return execute();
  }

  /**
   * Advance script to a given segment document.
   *
   * @param doc segment doc id
   * @return if there is data for the given id, this should always be the case
   */
  @Override
  public boolean advanceExact(int doc) {
    segmentDocLookup.setDocId(doc);
    docId = doc;
    scoreDocId = DOC_UNSET;
    return true;
  }

  /**
   * Get the score for the current document.
   *
   * @return document score
   */
  public double get_score() {
    try {
      if (scoreDocId == DOC_UNSET) {
        scoreDocId = docId;
        scores.advanceExact(scoreDocId);
      }
      return scores.doubleValue();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get score", e);
    }
  }

  /** Get the script parameters provided in the request. */
  public Map<String, Object> getParams() {
    return params;
  }

  /** Get doc values for the current document. */
  public Map<String, LoadedDocValues<?>> getDoc() {
    return segmentDocLookup;
  }

  /**
   * Factory required from the compilation of a ScoreScript. Used to produce request level {@link
   * DoubleValuesSource}. See script compile contract {@link ScriptContext}.
   */
  public interface Factory {
    /**
     * Create request level {@link DoubleValuesSource}.
     *
     * @param params parameters from script request
     * @param docLookup index level doc value lookup provider
     * @return {@link DoubleValuesSource} to evaluate script
     */
    DoubleValuesSource newFactory(Map<String, Object> params, DocLookup docLookup);
  }

  // compile context for the ScoreScript, contains script type info
  public static final ScriptContext<ScoreScript.Factory> CONTEXT =
      new ScriptContext<>(
          "score", ScoreScript.Factory.class, ScoreScript.SegmentFactory.class, ScoreScript.class);

  /**
   * Simple abstract implementation of a {@link DoubleValuesSource} this can be extended for engines
   * that need to implement a custom {@link ScoreScript}. The newInstance and needs_score must be
   * implemented. If more state is needed, the equals/hashCode should be redefined appropriately.
   *
   * <p>This class conforms with the script compile contract, see {@link ScriptContext}. However,
   * Engines are also free to create there own {@link DoubleValuesSource} implementations instead.
   */
  public abstract static class SegmentFactory extends DoubleValuesSource {
    private final Map<String, Object> params;
    private final DocLookup docLookup;

    public SegmentFactory(Map<String, Object> params, DocLookup docLookup) {
      this.params = params;
      this.docLookup = docLookup;
    }

    public Map<String, Object> getParams() {
      return params;
    }

    public DocLookup getDocLookup() {
      return docLookup;
    }

    /**
     * Create a {@link DoubleValues} instance for the given lucene segment.
     *
     * @param context segment context
     * @param scores provider of segment document scores
     * @return script to produce values for the given segment
     */
    public abstract DoubleValues newInstance(LeafReaderContext context, DoubleValues scores);

    /**
     * Get if this script will need access to the document score.
     *
     * @return if this script uses the document score.
     */
    public abstract boolean needs_score();

    /** Redirect {@link DoubleValuesSource} interface to script contract method. */
    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return newInstance(ctx, scores);
    }

    /** Redirect {@link DoubleValuesSource} interface to script contract method. */
    @Override
    public boolean needsScores() {
      return needs_score();
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(params, docLookup);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj.getClass() != this.getClass()) {
        return false;
      }
      SegmentFactory factory = (SegmentFactory) obj;
      return Objects.equals(factory.params, this.params)
          && Objects.equals(factory.docLookup, this.docLookup);
    }

    @Override
    public String toString() {
      return "ScoreScriptDoubleValuesSource: params: " + params + ", docLookup: " + docLookup;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      // It would be possible to cache this if we can verify no doc values for this segment have
      // been updated
      return false;
    }
  }
}
