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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues.SingleString;
import com.yelp.nrtsearch.server.luceneserver.suggest.protocol.ContextSuggestFieldData;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.search.suggest.document.SuggestIndexSearcher;

public class ContextSuggestFieldDef extends IndexableFieldDef {
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  /**
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected ContextSuggestFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    // if (requestField.getStore()) {
    //   throw new IllegalArgumentException("Context Suggest fields cannot be stored");
    // }

    // if (requestField.getSearch()) {
    //   throw new IllegalArgumentException("Context Suggest fields cannot be searched");
    // }
  }

  @Override
  public String getType() {
    return "CONTEXT_SUGGEST_FIELD";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    } else if (fieldValues.size() == 1) {
      ContextSuggestFieldData csfData =
          GSON.fromJson(fieldValues.get(0), ContextSuggestFieldData.class);
      CharSequence[] contexts =
          csfData.getContexts().toArray(new CharSequence[csfData.getContexts().size()]);
      ContextSuggestField csf =
          new ContextSuggestField(getName(), csfData.getValue(), csfData.getWeight(), contexts);
      document.add(csf);
    }
  }
}
