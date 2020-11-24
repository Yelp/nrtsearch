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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import java.util.List;
import org.apache.lucene.search.Query;

/**
 * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types that can
 * be queried by a {@link TermQuery} or {@link TermInSetQuery}.
 */
public interface TermQueryable {
  /**
   * Performs type validation. Do not @Override in subclasses.
   *
   * <p>Build a term query for this field type with the given configuration.
   *
   * @param termQuery term query configuration
   * @return lucene term query
   */
  default Query getTermQuery(TermQuery termQuery) {
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

  default Query getTermQueryFromBooleanValue(boolean booleanValue) {
    return null;
  }

  default Query getTermQueryFromDoubleValue(double doubleValue) {
    return null;
  }

  default Query getTermQueryFromFloatValue(float floatValue) {
    return null;
  }

  default Query getTermQueryFromIntValue(int intValue) {
    return null;
  }

  default Query getTermQueryFromLongValue(long longValue) {
    return null;
  }

  default Query getTermQueryFromTextValue(String textValue) {
    return null;
  }

  /**
   * Performs type validation. Do not @Override in subclasses.
   *
   * <p>Build a term in set query for this field type with the given configuration.
   *
   * @param termInSetQuery term in set query configuration
   * @return lucene term in set query
   */
  default Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
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

  default Query getTermInSetQueryFromDoubleValues(List<Double> doubleValues) {
    return null;
  }

  default Query getTermInSetQueryFromFloatValues(List<Float> floatValues) {
    return null;
  }

  default Query getTermInSetQueryFromIntValues(List<Integer> intValues) {
    return null;
  }

  default Query getTermInSetQueryFromLongValues(List<Long> longValues) {
    return null;
  }

  default Query getTermInSetQueryFromTextValues(List<String> textValues) {
    return null;
  }
}
