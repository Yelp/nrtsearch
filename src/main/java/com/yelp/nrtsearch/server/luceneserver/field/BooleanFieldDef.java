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

import com.yelp.nrtsearch.server.grpc.FacetType;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

/** Field class for 'BOOLEAN' field type. */
public class BooleanFieldDef extends IndexableFieldDef implements TermQueryable {
  protected BooleanFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (requestField.getHighlight()) {
      throw new IllegalArgumentException(
          String.format(
              "Field: %s cannot have highlight=true. only type=text or type=atom fields can have highlight=true",
              getName()));
    }

    if (hasAnalyzer(requestField) && !requestField.getSearch()) {
      throw new IllegalArgumentException("No analyzer allowed when search=false");
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues() || requestField.getSort() || requestField.getGroup()) {
      if (requestField.getMultiValued()) {
        return DocValuesType.SORTED_NUMERIC;
      } else {
        return DocValuesType.NUMERIC;
      }
    }
    return DocValuesType.NONE;
  }

  protected FacetValueType parseFacetValueType(Field requestField) {
    if (requestField.getFacet() == FacetType.HIERARCHY
        || requestField.getFacet() == FacetType.NUMERIC_RANGE
        || requestField.getFacet() == FacetType.SORTED_SET_DOC_VALUES
        || requestField.getFacet() == FacetType.FLAT) {
      throw new IllegalArgumentException("unsupported facet type: " + requestField.getFacet());
    }
    return FacetValueType.NO_FACETS;
  }

  @Override
  protected void setSearchProperties(FieldType fieldType, Field requestField) {
    if (requestField.getSearch()) {
      fieldType.setOmitNorms(true);
      fieldType.setTokenized(false);
      fieldType.setIndexOptions(IndexOptions.DOCS);
    }
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    }

    for (String fieldStr : fieldValues) {
      boolean value = parseBooleanOrThrow(fieldStr);
      int indexedValue;
      if (value) {
        indexedValue = 1;
      } else {
        indexedValue = 0;
      }

      if (hasDocValues()) {
        if (docValuesType == DocValuesType.NUMERIC) {
          document.add(new NumericDocValuesField(getName(), indexedValue));
        } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
          document.add(new SortedNumericDocValuesField(getName(), indexedValue));
        }
      }

      if (isStored() || isSearchable()) {
        document.add(new FieldWithData(getName(), fieldType, indexedValue));
      }
    }
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.NUMERIC) {
      NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), getName());
      return new LoadedDocValues.SingleBoolean(numericDocValues);
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(context.reader(), getName());
      return new LoadedDocValues.SortedBooleans(sortedNumericDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public String getType() {
    return "BOOLEAN";
  }

  @Override
  public Query getTermQueryFromBooleanValue(boolean booleanValue) {
    String indexTermValue = booleanValue ? "1" : "0";
    return new org.apache.lucene.search.TermQuery(new Term(getName(), indexTermValue));
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    boolean termValue = parseBooleanOrThrow(textValue);
    String indexTermValue = termValue ? "1" : "0";
    return new org.apache.lucene.search.TermQuery(new Term(getName(), indexTermValue));
  }

  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    // A boolean can only be two values, this query type is not very useful
    throw new UnsupportedOperationException("BOOLEAN fields do not support TermInSetQuery");
  }

  /**
   * Parses a String value into a boolean. The input String must match "true" or "false" (case
   * insensitive). Any other value is invalid and will result in an exception.
   *
   * @param booleanStr string to convert to boolean
   * @return boolean value represented by input string
   * @throws IllegalArgumentException if input string does not represent a boolean value
   */
  static boolean parseBooleanOrThrow(String booleanStr) {
    String lower = booleanStr.toLowerCase();
    if ("true".equals(lower)) {
      return true;
    } else if ("false".equals(lower)) {
      return false;
    } else {
      throw new IllegalArgumentException("Malformed boolean string: " + booleanStr);
    }
  }
}
