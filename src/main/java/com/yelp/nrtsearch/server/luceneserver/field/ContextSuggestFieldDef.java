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
package com.yelp.nrtsearch.server.luceneserver.field;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.luceneserver.suggest.protocol.ContextSuggestFieldData;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.suggest.document.ContextSuggestField;

public class ContextSuggestFieldDef extends IndexableFieldDef {
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();
  private final Analyzer indexAnalyzer;
  private final Analyzer searchAnalyzer;

  /**
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected ContextSuggestFieldDef(String name, Field requestField) {
    super(name, requestField);
    this.indexAnalyzer = this.parseIndexAnalyzer(requestField);
    this.searchAnalyzer = this.parseSearchAnalyzer(requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    if (requestField.getStoreDocValues()) {
      throw new IllegalArgumentException("Context Suggest fields cannot store doc values");
    }

    if (requestField.getSearch()) {
      throw new IllegalArgumentException("Context Suggest fields cannot be searched");
    }
  }

  @Override
  public String getType() {
    return "CONTEXT_SUGGEST";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (!isMultiValue() && fieldValues.size() > 1) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    }
    for (String fieldValue : fieldValues) {
      parseFieldValueToDocumentField(document, fieldValue);
    }
  }

  /**
   * Processes a single fieldValue and adds appropriate fields to the document.
   *
   * @param document document to add parsed values to
   * @param fieldValue string representation of the field value
   */
  private void parseFieldValueToDocumentField(Document document, String fieldValue) {
    ContextSuggestFieldData csfData = GSON.fromJson(fieldValue, ContextSuggestFieldData.class);
    CharSequence[] contexts =
        csfData.getContexts().toArray(new CharSequence[csfData.getContexts().size()]);
    ContextSuggestField csf =
        new ContextSuggestField(getName(), csfData.getValue(), csfData.getWeight(), contexts);
    document.add(csf);
  }

  protected Analyzer parseIndexAnalyzer(Field requestField) {
    if (AnalyzerCreator.isAnalyzerDefined(requestField.getAnalyzer())) {
      return AnalyzerCreator.getInstance().getAnalyzer(requestField.getAnalyzer());
    } else if (AnalyzerCreator.isAnalyzerDefined(requestField.getIndexAnalyzer())) {
      return AnalyzerCreator.getInstance().getAnalyzer(requestField.getIndexAnalyzer());
    } else {
      return AnalyzerCreator.getStandardAnalyzer();
    }
  }

  protected Analyzer parseSearchAnalyzer(Field requestField) {
    if (AnalyzerCreator.isAnalyzerDefined(requestField.getAnalyzer())) {
      return AnalyzerCreator.getInstance().getAnalyzer(requestField.getAnalyzer());
    } else if (AnalyzerCreator.isAnalyzerDefined(requestField.getSearchAnalyzer())) {
      return AnalyzerCreator.getInstance().getAnalyzer(requestField.getSearchAnalyzer());
    } else {
      return AnalyzerCreator.getStandardAnalyzer();
    }
  }

  public Optional<Analyzer> getIndexAnalyzer() {
    return Optional.ofNullable(this.indexAnalyzer);
  }

  public Optional<Analyzer> getSearchAnalyzer() {
    return Optional.ofNullable(this.searchAnalyzer);
  }

  public String getPostingsFormat() {
    return "Completion84";
  }
}
