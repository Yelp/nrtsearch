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
    switch (termQuery.getTermTypesCase()) {
      case BOOLEANVALUE:
        return getTermQueryFromBooleanValue(termQuery);
      case DOUBLEVALUE:
        return getTermQueryFromDoubleValue(termQuery);
      case FLOATVALUE:
        return getTermQueryFromFloatValue(termQuery);
      case INTVALUE:
        return getTermQueryFromIntValue(termQuery);
      case LONGVALUE:
        return getTermQueryFromLongValue(termQuery);
      case TEXTVALUE:
        return getTermQueryFromTextValue(termQuery);
      default:
        throw new IllegalArgumentException(
            String.format(
                "%s field does not support term type: %s",
                termQuery.getField(), termQuery.getTermTypesCase()));
    }
  }

  Query getTermQueryFromBooleanValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  Query getTermQueryFromDoubleValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  Query getTermQueryFromFloatValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  Query getTermQueryFromIntValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  Query getTermQueryFromLongValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  Query getTermQueryFromTextValue(TermQuery termQuery) {
    throwIllegalArgumentException(termQuery);
    return null;
  }

  void throwIllegalArgumentException(TermQuery termQuery) {
    throw new IllegalArgumentException(
        String.format(
            "%s field does not support term type: %s",
            termQuery.getField(), termQuery.getTermTypesCase()));
  }

  // Performs type validation. Do not @Override in subclasses.
  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    switch (termInSetQuery.getTermTypesCase()) {
      case DOUBLETERMS:
        return getTermInSetQueryFromDoubleValue(termInSetQuery);
      case FLOATTERMS:
        return getTermInSetQueryFromFloatValue(termInSetQuery);
      case INTTERMS:
        return getTermInSetQueryFromIntValue(termInSetQuery);
      case LONGTERMS:
        return getTermInSetQueryFromLongValue(termInSetQuery);
      case TEXTTERMS:
        return getTermInSetQueryFromTextValue(termInSetQuery);
      default:
        throw new IllegalArgumentException(
            String.format(
                "%s field does not support term type: %s",
                termInSetQuery.getField(), termInSetQuery.getTermTypesCase()));
    }
  }

  Query getTermInSetQueryFromDoubleValue(TermInSetQuery termInSetQuery) {
    throwIllegalArgumentException(termInSetQuery);
    return null;
  }

  Query getTermInSetQueryFromFloatValue(TermInSetQuery termInSetQuery) {
    throwIllegalArgumentException(termInSetQuery);
    return null;
  }

  Query getTermInSetQueryFromIntValue(TermInSetQuery termInSetQuery) {
    throwIllegalArgumentException(termInSetQuery);
    return null;
  }

  Query getTermInSetQueryFromLongValue(TermInSetQuery termInSetQuery) {
    throwIllegalArgumentException(termInSetQuery);
    return null;
  }

  Query getTermInSetQueryFromTextValue(TermInSetQuery termInSetQuery) {
    throwIllegalArgumentException(termInSetQuery);
    return null;
  }

  void throwIllegalArgumentException(TermInSetQuery termInSetQuery)
      throws IllegalArgumentException {
    throw new IllegalArgumentException(
        String.format(
            "%s field does not support term type: %s",
            termInSetQuery.getField(), termInSetQuery.getTermTypesCase()));
  }
}
