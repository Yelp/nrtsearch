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
import com.yelp.nrtsearch.server.luceneserver.script.RuntimeScript.SegmentFactory;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Script to produce a value for a given document. Implementations must have an execute function.
 * This class conforms with the script compile contract, see {@link ScriptContext}. The script has
 * access to the query parameters, the document doc values through {@link SegmentDocLookup}.
 */
public abstract class RuntimeScript {
  private final Map<String, Object> params;
  private final SegmentDocLookup segmentDocLookup;

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

  /** Factory interface for creating a RuntimeSsrc/main/java/com/yelp/nrtsearch/server/luceneserver/script/RuntimeScript.java cript bound to a lucene segment. */
  public interface SegmentFactory {

    /**
     * Create a RuntimeScript instance for a lucene segment.
     *
     * @param context lucene segment context
     * @return segment level RuntimeScript
     * @throws IOException
     */
    RuntimeScript newInstance(LeafReaderContext context) throws IOException;
  }

  /**
   * Factory required for the compilation of a RuntimeScript. Used to produce request level {@link
   * SegmentFactory}. See script compile contract {@link ScriptContext}.
   */
  public interface Factory {
    /**
     * Create request level {@link RuntimeScript.SegmentFactory}.
     *
     * @param params parameters from script request
     * @param docLookup index level doc value lookup provider
     * @return {@link RuntimeScript.SegmentFactory} to evaluate script
     */
    SegmentFactory newFactory(Map<String, Object> params, DocLookup docLookup);
  }

  // compile context for the RuntimeScript, contains script type info
  public static final ScriptContext<Factory> CONTEXT =
      new ScriptContext<>("runtime", Factory.class, SegmentFactory.class, RuntimeScript.class);
}
