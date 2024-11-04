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
package com.yelp.nrtsearch.server.field.properties;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import java.util.List;
import org.apache.lucene.search.Query;

/**
 * Trait interface for {@link FieldDef} types that can be queried by a {@link TermQuery} or {@link
 * TermInSetQuery}.
 */
public interface TermQueryable {
  /**
   * Build a term query for this field type with the given configuration.
   *
   * <p>Performs type validation. Do not @Override in subclasses.
   *
   * @param termQuery term query configuration
   * @return lucene term query
   * @throws UnsupportedOperationException if field does not support term type
   */
  default Query getTermQuery(TermQuery termQuery) {
    Query query =
        switch (termQuery.getTermTypesCase()) {
          case BOOLEANVALUE -> getTermQueryFromBooleanValue(termQuery.getBooleanValue());
          case DOUBLEVALUE -> getTermQueryFromDoubleValue(termQuery.getDoubleValue());
          case FLOATVALUE -> getTermQueryFromFloatValue(termQuery.getFloatValue());
          case INTVALUE -> getTermQueryFromIntValue(termQuery.getIntValue());
          case LONGVALUE -> getTermQueryFromLongValue(termQuery.getLongValue());
          case TEXTVALUE -> getTermQueryFromTextValue(termQuery.getTextValue());
          default -> null;
        };

    if (query == null) {
      throw new UnsupportedOperationException(
          String.format(
              "%s field does not support term type: %s",
              termQuery.getField(), termQuery.getTermTypesCase()));
    } else {
      return query;
    }
  }

  /**
   * Build a term query with a boolean value.
   *
   * <p>@Override in subclasses if boolean is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param booleanValue boolean value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromBooleanValue(boolean booleanValue) {
    return null;
  }

  /**
   * Build a term query with a double value.
   *
   * <p>@Override in subclasses if double is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param doubleValue double value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromDoubleValue(double doubleValue) {
    return null;
  }

  /**
   * Build a term query with a float value.
   *
   * <p>@Override in subclasses if float is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param floatValue float value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromFloatValue(float floatValue) {
    return null;
  }

  /**
   * Build a term query with a integer value.
   *
   * <p>@Override in subclasses if integer is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param intValue integer value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromIntValue(int intValue) {
    return null;
  }

  /**
   * Build a term query with a long value.
   *
   * <p>@Override in subclasses if long is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param longValue long value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromLongValue(long longValue) {
    return null;
  }

  /**
   * Build a term query with a String value.
   *
   * <p>@Override in subclasses if String is an acceptable type for the FieldDef's term query.
   *
   * <p>If not overridden, this method will return null and getTermQuery(termQuery) will throw
   * UnsupportedOperationException.
   *
   * @param textValue String value for term query
   * @return null, should be overridden in subclasses to return lucene term query
   */
  default Query getTermQueryFromTextValue(String textValue) {
    return null;
  }

  /**
   * Build a term in set query for this field type with the given configuration.
   *
   * <p>Performs type validation. Do not @Override in subclasses.
   *
   * @param termInSetQuery term in set query configuration
   * @return lucene term in set query
   * @throws UnsupportedOperationException if field does not support term type
   */
  default Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    Query query =
        switch (termInSetQuery.getTermTypesCase()) {
          case DOUBLETERMS ->
              getTermInSetQueryFromDoubleValues(termInSetQuery.getDoubleTerms().getTermsList());
          case FLOATTERMS ->
              getTermInSetQueryFromFloatValues(termInSetQuery.getFloatTerms().getTermsList());
          case INTTERMS ->
              getTermInSetQueryFromIntValues(termInSetQuery.getIntTerms().getTermsList());
          case LONGTERMS ->
              getTermInSetQueryFromLongValues(termInSetQuery.getLongTerms().getTermsList());
          case TEXTTERMS ->
              getTermInSetQueryFromTextValues(termInSetQuery.getTextTerms().getTermsList());
          default -> null;
        };
    if (query == null) {
      throw new UnsupportedOperationException(
          String.format(
              "%s field does not support term type: %s",
              termInSetQuery.getField(), termInSetQuery.getTermTypesCase()));
    } else {
      return query;
    }
  }

  /**
   * Build a term in set query with a list of double values.
   *
   * <p>@Override in subclasses if a list of doubles is an acceptable type for the FieldDef's term
   * in set query.
   *
   * <p>If not overridden, this method will return null and getTermInSetQuery(termInSetQuery) will
   * throw UnsupportedOperationException.
   *
   * @param doubleValues List of double values for term query
   * @return null, should be overridden in subclasses to return lucene term in set query
   */
  default Query getTermInSetQueryFromDoubleValues(List<Double> doubleValues) {
    return null;
  }

  /**
   * Build a term in set query with a list of float values.
   *
   * <p>@Override in subclasses if a list of floats is an acceptable type for the FieldDef's term in
   * set query.
   *
   * <p>If not overridden, this method will return null and getTermInSetQuery(termInSetQuery) will
   * throw UnsupportedOperationException.
   *
   * @param floatValues List of float values for term query
   * @return null, should be overridden in subclasses to return lucene term in set query
   */
  default Query getTermInSetQueryFromFloatValues(List<Float> floatValues) {
    return null;
  }

  /**
   * Build a term in set query with a list of integer values.
   *
   * <p>@Override in subclasses if a list of integers is an acceptable type for the FieldDef's term
   * in set query.
   *
   * <p>If not overridden, this method will return null and getTermInSetQuery(termInSetQuery) will
   * throw UnsupportedOperationException.
   *
   * @param intValues List of integer values for term query
   * @return null, should be overridden in subclasses to return lucene term in set query
   */
  default Query getTermInSetQueryFromIntValues(List<Integer> intValues) {
    return null;
  }

  /**
   * Build a term in set query with a list of long values.
   *
   * <p>@Override in subclasses if a list of longs is an acceptable type for the FieldDef's term in
   * set query.
   *
   * <p>If not overridden, this method will return null and getTermInSetQuery(termInSetQuery) will
   * throw UnsupportedOperationException.
   *
   * @param longValues List of long values for term query
   * @return null, should be overridden in subclasses to return lucene term in set query
   */
  default Query getTermInSetQueryFromLongValues(List<Long> longValues) {
    return null;
  }

  /**
   * Build a term in set query with a list of String values.
   *
   * <p>@Override in subclasses if a list of Strings is an acceptable type for the FieldDef's term
   * in set query.
   *
   * <p>If not overridden, this method will return null and getTermInSetQuery(termInSetQuery) will
   * throw UnsupportedOperationException.
   *
   * @param textValues List of String values for term query
   * @return null, should be overridden in subclasses to return lucene term in set query
   */
  default Query getTermInSetQueryFromTextValues(List<String> textValues) {
    return null;
  }
}
