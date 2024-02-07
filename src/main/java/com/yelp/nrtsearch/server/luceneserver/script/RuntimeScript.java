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
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * Script to produce a value for a given document. Implementations must have an execute function.
 * This class conforms with the script compile contract, see {@link ScriptContext}. The script has
 * access to the query parameters, the document doc values through {@link SegmentDocLookup}.
 */
public abstract class RuntimeScript {

  private static final int DOC_UNSET = -1;
  private final Map<String, Object> params;
  private final SegmentDocLookup segmentDocLookup;

  private Object obj;

  private int docId = DOC_UNSET;
  private int scoreDocId = DOC_UNSET;

  // names for parameters to execute
  public static final String[] PARAMETERS = new String[] {};

  /**
   * ObjectScript constructor.
   *
   * @param params script parameters from {@link com.yelp.nrtsearch.server.grpc.Script}
   * @param docLookup index level doc values lookup
   * @param leafContext lucene segment context
   */
  public RuntimeScript(
      Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
    this.params = params;
    this.segmentDocLookup = docLookup.getSegmentLookup(leafContext);
  }

  /**
   * Main script function.
   *
   * @return object value computed for document
   */
  public abstract Object execute();

  /** Set segment level id for the next document to score. * */
  public void setDocId(int docId) {
    segmentDocLookup.setDocId(docId);
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
   * Simple abstract implementation of a {@link ValueSource} this can be extended for engines that
   * need to implement a custom {@link RuntimeScript}. The newInstance and needs_score must be
   * implemented. If more state is needed, the equals/hashCode should be redefined appropriately.
   *
   * <p>This class conforms with the script compile contract, see {@link ScriptContext}. However,
   * Engines are also free to create there own {@link ValueSource} implementations instead.
   */
  public abstract static class SegmentFactory extends ValueSource {
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
     * Create a {@link FunctionValues} instance for the given lucene segment.
     *
     * @param ctx Context map for
     * @param context segment context
     * @return script to produce values for the given segment
     */
    public abstract FunctionValues newInstance(Map ctx, LeafReaderContext context);

    /**
     * Get if this script will need access to the document score.
     *
     * @return if this script uses the document score.
     */
    public abstract boolean needs_score();

    /** Redirect {@link ValueSource} interface to script contract method. */
    @Override
    public FunctionValues getValues(Map context, LeafReaderContext ctx) throws IOException {
      return newInstance(context, ctx);
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
      RuntimeScript.SegmentFactory factory = (RuntimeScript.SegmentFactory) obj;
      return Objects.equals(factory.params, this.params)
          && Objects.equals(factory.docLookup, this.docLookup);
    }

    @Override
    public String toString() {
      return "RuntimeScriptValuesSource: params: " + params + ", docLookup: " + docLookup;
    }
  }

  /**
   * Factory required from the compilation of a ScoreScript. Used to produce request level {@link
   * ValueSource}. See script compile contract {@link ScriptContext}.
   */
  public interface Factory {
    /**
     * Create request level {@link SegmentFactory}.
     *
     * @param params parameters from script request
     * @param docLookup index level doc value lookup provider
     * @return {@link ValueSource} to evaluate script
     */
    ValueSource newFactory(Map<String, Object> params, DocLookup docLookup);
  }

  /**
   * Advance script to a given segment document.
   *
   * @param doc segment doc id
   * @return if there is data for the given id, this should always be the case
   */
  public boolean advanceExact(int doc) {
    segmentDocLookup.setDocId(doc);
    docId = doc;
    scoreDocId = DOC_UNSET;
    return true;
  }

  // compile context for the ScoreScript, contains script type info
  public static final ScriptContext<Factory> CONTEXT =
      new ScriptContext<>(
          "runtime", Factory.class, RuntimeScript.SegmentFactory.class, RuntimeScript.class);
}
