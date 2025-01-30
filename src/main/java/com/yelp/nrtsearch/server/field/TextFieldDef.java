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
package com.yelp.nrtsearch.server.field;

import com.yelp.nrtsearch.server.field.properties.PrefixQueryable;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

/** Field class for 'TEXT' field type. */
public class TextFieldDef extends TextBaseFieldDef implements PrefixQueryable {
  protected PrefixFieldDef prefixFieldDef;
  private Map<String, IndexableFieldDef<?>> childFieldsWithPrefix;

  public TextFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    super(name, requestField, context);
    if (requestField.hasIndexPrefixes()) {
      if (!requestField.getSearch()) {
        throw new IllegalArgumentException(
            "Cannot set index_prefixes on unindexed field [" + name + "]");
      }
      this.prefixFieldDef = new PrefixFieldDef(getName(), requestField, context);
      Map<String, IndexableFieldDef<?>> childFieldsMap = new HashMap<>(super.getChildFields());
      childFieldsMap.put(prefixFieldDef.getName(), prefixFieldDef);
      childFieldsWithPrefix = Collections.unmodifiableMap(childFieldsMap);
    } else {
      this.prefixFieldDef = null;
      childFieldsWithPrefix = super.getChildFields();
    }
  }

  @Override
  public Map<String, IndexableFieldDef<?>> getChildFields() {
    return childFieldsWithPrefix;
  }

  @Override
  public String getType() {
    return "TEXT";
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    if (requestField.getSearch()) {
      setIndexOptions(
          requestField.getIndexOptions(), fieldType, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

      switch (requestField.getTermVectors()) {
        case TERMS_POSITIONS_OFFSETS_PAYLOADS:
          fieldType.setStoreTermVectorPayloads(true);
        case TERMS_POSITIONS_OFFSETS:
          fieldType.setStoreTermVectorOffsets(true);
        case TERMS_POSITIONS:
          fieldType.setStoreTermVectorPositions(true);
        case TERMS:
          fieldType.setStoreTermVectors(true);
      }
    }
    fieldType.setTokenized(true);
    fieldType.setOmitNorms(requestField.getOmitNorms());
  }

  public PrefixFieldDef getPrefixFieldDef() {
    return prefixFieldDef;
  }

  public boolean hasPrefix() {
    return prefixFieldDef != null;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    super.parseDocumentField(document, fieldValues, facetHierarchyPaths);

    if (hasPrefix() && !fieldValues.isEmpty()) {
      prefixFieldDef.parseDocumentField(document, fieldValues);
    }
  }

  @Override
  public Query getPrefixQuery(PrefixQuery prefixQuery, MultiTermQuery.RewriteMethod rewriteMethod) {
    if (prefixFieldDef != null && prefixFieldDef.accept(prefixQuery.getPrefix().length())) {
      Query query = prefixFieldDef.getPrefixQuery(prefixQuery);
      if (rewriteMethod == null
          || rewriteMethod == MultiTermQuery.CONSTANT_SCORE_REWRITE
          || rewriteMethod == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
        return new ConstantScoreQuery(query);
      }
      return query;
    }
    return new org.apache.lucene.search.PrefixQuery(
        new Term(prefixQuery.getField(), prefixQuery.getPrefix()), rewriteMethod);
  }
}
