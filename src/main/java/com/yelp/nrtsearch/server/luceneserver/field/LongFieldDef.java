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
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'LONG' field type. */
public class LongFieldDef extends NumberFieldDef {

  public LongFieldDef(String name, Field requestField) {
    super(name, requestField, LONG_PARSER);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new NumericDocValuesField(getName(), fieldValue.longValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(getName(), fieldValue.longValue());
    }
    throw new IllegalStateException("Unsupported doc value type: " + docValuesType);
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new LongPoint(getName(), fieldValue.longValue());
  }

  @Override
  protected LoadedDocValues<?> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleLong(docValues);
  }

  @Override
  protected LoadedDocValues<?> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedLongs(docValues);
  }

  @Override
  protected DoubleValuesSource getBindingSource() {
    return DoubleValuesSource.fromLongField(getName());
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.LONG;
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Long.MAX_VALUE : Long.MIN_VALUE;
  }

  @Override
  public String getType() {
    return "LONG";
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    long lower =
        rangeQuery.getLower().isEmpty() ? Long.MIN_VALUE : Long.parseLong(rangeQuery.getLower());
    long upper =
        rangeQuery.getUpper().isEmpty() ? Long.MAX_VALUE : Long.parseLong(rangeQuery.getUpper());
    ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

    Query pointQuery = LongPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

    if (!hasDocValues()) {
      return pointQuery;
    }

    Query dvQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(), lower, upper);
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, long lower, long upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  public Query getTermQuery(TermQuery termQuery) {
    return LongPoint.newExactQuery(getName(), termQuery.getLongValue());
  }

  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    return LongPoint.newSetQuery(getName(), termInSetQuery.getLongTerms().getTermsList());
  }
}
