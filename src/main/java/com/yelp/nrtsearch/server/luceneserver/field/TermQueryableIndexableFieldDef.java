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

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.field.properties.TermQueryable;
import java.util.List;
import org.apache.lucene.search.Query;

public abstract class TermQueryableIndexableFieldDef extends IndexableFieldDef
    implements TermQueryable {
  /**
   * Field constructor. Performs generalized building of field definition by calling a number of
   * protected methods, mainly: {@link #validateRequest(Field)}, {@link #parseDocValuesType(Field)},
   * {@link #parseFacetValueType(Field)}, and {@link #setSearchProperties(FieldType, Field)}.
   * Concrete field types should override these methods as needed.
   *
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected TermQueryableIndexableFieldDef(String name, Field requestField) {
    super(name, requestField);
  }

  // Performs type validation. Do not @Override in subclasses.
  @Override
  public Query getTermQuery(TermQuery termQuery) {
    Query query;
    switch (termQuery.getTermTypesCase()) {
      case BOOLEANVALUE:
        query = getTermQueryFromBooleanValue(termQuery.getBooleanValue());
        break;
      case DOUBLEVALUE:
        query = getTermQueryFromDoubleValue(termQuery.getDoubleValue());
        break;
      case FLOATVALUE:
        query = getTermQueryFromFloatValue(termQuery.getFloatValue());
        break;
      case INTVALUE:
        query = getTermQueryFromIntValue(termQuery.getIntValue());
        break;
      case LONGVALUE:
        query = getTermQueryFromLongValue(termQuery.getLongValue());
        break;
      case TEXTVALUE:
        query = getTermQueryFromTextValue(termQuery.getTextValue());
        break;
      default:
        query = null;
        break;
    }

    if (query.equals(null)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s field does not support term type: %s",
              termQuery.getField(), termQuery.getTermTypesCase()));
    } else {
      return query;
    }
  }

  Query getTermQueryFromBooleanValue(boolean booleanValue) {
    return null;
  }

  Query getTermQueryFromDoubleValue(double doubleValue) {
    return null;
  }

  Query getTermQueryFromFloatValue(float floatValue) {
    return null;
  }

  Query getTermQueryFromIntValue(int intValue) {
    return null;
  }

  Query getTermQueryFromLongValue(long longValue) {
    return null;
  }

  Query getTermQueryFromTextValue(String textValue) {
    return null;
  }

  // Performs type validation. Do not @Override in subclasses.
  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    Query query;
    switch (termInSetQuery.getTermTypesCase()) {
      case DOUBLETERMS:
        query = getTermInSetQueryFromDoubleValues(termInSetQuery.getDoubleTerms().getTermsList());
        break;
      case FLOATTERMS:
        query = getTermInSetQueryFromFloatValues(termInSetQuery.getFloatTerms().getTermsList());
        break;
      case INTTERMS:
        query = getTermInSetQueryFromIntValues(termInSetQuery.getIntTerms().getTermsList());
        break;
      case LONGTERMS:
        query = getTermInSetQueryFromLongValues(termInSetQuery.getLongTerms().getTermsList());
        break;
      case TEXTTERMS:
        query = getTermInSetQueryFromTextValues(termInSetQuery.getTextTerms().getTermsList());
        break;
      default:
        query = null;
        break;
    }
    if (query.equals(null)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s field does not support term type: %s",
              termInSetQuery.getField(), termInSetQuery.getTermTypesCase()));
    } else {
      return query;
    }
  }

  Query getTermInSetQueryFromDoubleValues(List<Double> doubleValues) {
    return null;
  }

  Query getTermInSetQueryFromFloatValues(List<Float> floatValues) {
    return null;
  }

  Query getTermInSetQueryFromIntValues(List<Integer> intValues) {
    return null;
  }

  Query getTermInSetQueryFromLongValues(List<Long> longValues) {
    return null;
  }

  Query getTermInSetQueryFromTextValues(List<String> textValues) {
    return null;
  }
}
