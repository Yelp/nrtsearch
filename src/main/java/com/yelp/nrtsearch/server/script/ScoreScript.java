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
package com.yelp.nrtsearch.server.script;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.doc.SegmentDocLookup;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

/**
 * Script to produce a double value for a given document. Implementations must provide an {@link
 * #execute()} method. This class conforms with the script compile contract defined by {@link
 * ScriptContext}.
 *
 * <p>Scripts have access to:
 *
 * <ul>
 *   <li><b>Parameters</b> — via {@link #getParams()}, set in the {@link
 *       com.yelp.nrtsearch.server.grpc.Script} request.
 *   <li><b>Doc values</b> — via {@link #getDoc()}, backed by {@link SegmentDocLookup}.
 *   <li><b>Previous-pass score</b> — via {@link #get_score()}.
 *   <li><b>Shared doc context values</b> — per-document values contributed by earlier pipeline
 *       steps, accessed via {@link #getSharedDocContext()}. For multi-retriever queries each
 *       retriever's raw score is available under the key {@code retriever_<name>}.
 * </ul>
 */
public abstract class ScoreScript extends DoubleValues {
  private static final int DOC_UNSET = -1;

  private final Map<String, Object> params;
  private final SegmentDocLookup segmentDocLookup;
  private final DoubleValues scores;
  private final SharedDocContext sharedDocContext;
  private int docId = DOC_UNSET;
  private int scoreDocId = DOC_UNSET;
  private int leafDocBase = 0;

  // names for parameters to execute
  public static final String[] PARAMETERS = new String[] {};

  /**
   * ScoreScript constructor.
   *
   * @param params script parameters from {@link com.yelp.nrtsearch.server.grpc.Script}
   * @param docLookup index level doc values lookup
   * @param leafContext lucene segment context
   * @param scores provider of segment document scores
   * @param sharedDocContext per-document context shared across pipeline stages, or {@code null}
   */
  public ScoreScript(
      Map<String, Object> params,
      DocLookup docLookup,
      LeafReaderContext leafContext,
      DoubleValues scores,
      SharedDocContext sharedDocContext) {
    this.params = params;
    this.segmentDocLookup = docLookup.getSegmentLookup(leafContext);
    this.scores = scores;
    this.sharedDocContext = sharedDocContext;
    this.leafDocBase = leafContext.docBase;
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

  /**
   * Get the shared doc context map for the current document. Allows script implementations to
   * access per-document values contributed by earlier pipeline stages.
   *
   * <p>For multi-retriever queries, each retriever's raw score is stored under {@code
   * retriever_<name>} (e.g. {@code "retriever_text"}, {@code "retriever_knn"}).
   *
   * <p>JS expressions access these values via the {@code _shared_<key>} variable convention, which
   * casts to double internally. ScoreScript subclasses use this method directly.
   *
   * @return shared doc context map for the current document, or an empty map if no context exists
   */
  public Map<String, Object> getSharedDocContext() {
    if (sharedDocContext == null) {
      return Map.of();
    }
    return sharedDocContext.getContext(leafDocBase + docId);
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
     * @param context request-level resources available to the script factory
     * @return {@link DoubleValuesSource} to evaluate script
     */
    DoubleValuesSource newFactory(ScriptFactoryContext context);
  }

  // compile context for the ScoreScript, contains script type info
  public static final ScriptContext<ScoreScript.Factory> CONTEXT =
      new ScriptContext<>(
          "score", ScoreScript.Factory.class, ScoreScript.SegmentFactory.class, ScoreScript.class);

  /**
   * Abstract {@link DoubleValuesSource} that script engines extend to produce per-segment {@link
   * ScoreScript} instances. Subclasses must implement {@link #newInstance} and {@link
   * #needs_score}. If additional state is held, {@link #equals} and {@link #hashCode} should be
   * overridden accordingly.
   *
   * <p>This class conforms with the script compile contract, see {@link ScriptContext}. However,
   * Engines are also free to create their own {@link DoubleValuesSource} implementations instead.
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
