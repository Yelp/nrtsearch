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
import org.apache.lucene.index.LeafReaderContext;

/**
 * Script to produce an Object value used for Facet aggregations. This Object may be a single value,
 * or an Iterable of values. Implementations must have an execute function. This class conforms with
 * the script compile contract, see {@link ScriptContext}. The script has access to the query
 * parameters and the document doc values through {@link SegmentDocLookup}.
 */
public abstract class FacetScript {

  private final Map<String, Object> params;
  private final SegmentDocLookup segmentDocLookup;

  // names for parameters to execute
  public static final String[] PARAMETERS = new String[] {};

  /**
   * FacetScript constructor.
   *
   * @param params script parameters from {@link com.yelp.nrtsearch.server.grpc.Script}
   * @param docLookup index level doc values lookup
   * @param leafContext lucene segment context
   */
  public FacetScript(
      Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
    this.params = params;
    this.segmentDocLookup = docLookup.getSegmentLookup(leafContext);
  }

  /**
   * Main script function.
   *
   * @return Object value computed for document
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

  /** Factory interface for creating a FacetScript bound to a lucene segment. */
  public interface SegmentFactory {

    /**
     * Create a FacetScript instance for a lucene segement.
     *
     * @param context lucene segment context
     * @return segment level FacetScript
     * @throws IOException
     */
    FacetScript newInstance(LeafReaderContext context) throws IOException;
  }

  /**
   * Factory required for the compilation of a FacetScript. Used to produce request level {@link
   * SegmentFactory}. See script compile contract {@link ScriptContext}.
   */
  public interface Factory {
    /**
     * Create request level {@link SegmentFactory}.
     *
     * @param params parameters from script request
     * @param docLookup index level doc value lookup provider
     * @return {@link SegmentFactory} to evaluate script
     */
    SegmentFactory newFactory(Map<String, Object> params, DocLookup docLookup);
  }

  // compile context for the FacetScript, contains script type info
  public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("facet", Factory.class);
}
