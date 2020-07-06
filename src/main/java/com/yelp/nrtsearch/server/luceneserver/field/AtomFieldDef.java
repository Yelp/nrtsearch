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
package com.yelp.nrtsearch.server.luceneserver.field;

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.hasAnalyzer;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;

/** Field class for 'ATOM' field type. Uses {@link KeywordAnalyzer} for text analysis. */
public class AtomFieldDef extends TextBaseFieldDef implements Sortable {
  private static final Analyzer keywordAnalyzer = new KeywordAnalyzer();

  public AtomFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    if (requestField.getHighlight() && !requestField.getSearch()) {
      throw new IllegalArgumentException("search must be true when highlight is true");
    }

    if (requestField.getMultiValued() && requestField.getGroup()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s cannot have both group and multivalued set to true. Cannot group on multiValued fields",
              requestField.getName()));
    }

    if (requestField.getHighlight() && !requestField.getStore()) {
      throw new IllegalArgumentException("store must be true when highlight is true");
    }

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException(
          "no analyzer allowed with atom (it's hardwired to KeywordAnalyzer internally)");
    }
  }

  @Override
  public String getType() {
    return "ATOM";
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getSort() || requestField.getGroup()) {
      if (requestField.getMultiValued()) {
        return DocValuesType.SORTED_SET;
      } else {
        return DocValuesType.SORTED;
      }
    } else if (requestField.getStoreDocValues()) {
      // needed to support multivalued text fields even though its not grouped
      // since neither BINARY nor SORTED allows for multiValued fields during indexing
      if (requestField.getMultiValued()) {
        return DocValuesType.SORTED_SET;
      } else {
        return DocValuesType.BINARY;
      }
    }
    return DocValuesType.NONE;
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    // This is how things are currently expected, if the field is searchable it
    // gets the same properties set as a text field. Even if it is not set
    // searchable, it is expected to at least have the DOCS index option so
    // it can be deleted by Term. This doesn't seem right and we should think
    // about changing it.
    if (requestField.getSearch()) {
      super.setSearchProperties(fieldType, requestField);
    } else {
      fieldType.setOmitNorms(true);
      fieldType.setTokenized(false);
      fieldType.setIndexOptions(IndexOptions.DOCS);
    }
  }

  @Override
  public boolean isSearchable() {
    // even if the search property is not set this field may have index options,
    // see setSearchProperties.
    return super.isSearchable() || fieldType.indexOptions() != IndexOptions.NONE;
  }

  @Override
  protected Analyzer parseIndexAnalyzer(Field requestField) {
    return keywordAnalyzer;
  }

  @Override
  protected Analyzer parseSearchAnalyzer(Field requestField) {
    return keywordAnalyzer;
  }

  @Override
  public SortField getSortField(SortType type) {
    if (!hasDocValues()) {
      throw new IllegalStateException("Doc values are required for sorted fields");
    }
    SortField sortField;
    if (isMultiValue()) {
      sortField =
          new SortedSetSortField(
              getName(), type.getReverse(), SORTED_SET_TYPE_PARSER.apply(type.getSelector()));
    } else {
      sortField = new SortField(getName(), SortField.Type.STRING, type.getReverse());
    }

    boolean missingLast = type.getMissingLat();
    if (missingLast) {
      sortField.setMissingValue(SortField.STRING_LAST);
    } else {
      sortField.setMissingValue(SortField.STRING_FIRST);
    }
    return sortField;
  }
}
