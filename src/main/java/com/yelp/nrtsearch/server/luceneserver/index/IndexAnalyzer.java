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
package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.luceneserver.field.ContextSuggestFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.TextBaseFieldDef;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * Analyzer wrapper for use at indexing time, which finds the proper per field analyzer. This
 * analyzer is set in the {@link org.apache.lucene.index.IndexWriterConfig} when an index is
 * started. The {@link IndexStateManager} provides access to the fields in the latest {@link
 * com.yelp.nrtsearch.server.luceneserver.IndexState}.
 */
public class IndexAnalyzer extends AnalyzerWrapper {
  private final IndexStateManager stateManager;

  protected IndexAnalyzer(IndexStateManager stateManager) {
    super(Analyzer.PER_FIELD_REUSE_STRATEGY);
    this.stateManager = stateManager;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String name) {
    FieldDef fd = stateManager.getCurrent().getField(name);
    if (fd instanceof TextBaseFieldDef || fd instanceof ContextSuggestFieldDef) {
      Optional<Analyzer> maybeAnalyzer;
      if (fd instanceof TextBaseFieldDef) {
        maybeAnalyzer = ((TextBaseFieldDef) fd).getIndexAnalyzer();
      } else {
        maybeAnalyzer = ((ContextSuggestFieldDef) fd).getIndexAnalyzer();
      }

      if (maybeAnalyzer.isEmpty()) {
        throw new IllegalArgumentException(
            "field \"" + name + "\" did not specify analyzer or indexAnalyzer");
      }
      return maybeAnalyzer.get();
    }
    throw new IllegalArgumentException("field \"" + name + "\" does not support analysis");
  }
}
