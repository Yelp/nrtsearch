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
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.NumericUtils;

/** Field class for 'DOUBLE' field type. */
public class DoubleFieldDef extends NumberFieldDef {

  public DoubleFieldDef(String name, Field requestField) {
    super(name, requestField, DOUBLE_PARSER);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new DoubleDocValuesField(getName(), fieldValue.doubleValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(
          getName(), SORTED_DOUBLE_ENCODER.applyAsLong(fieldValue));
    }
    throw new IllegalStateException("Unsupported doc value type: " + docValuesType);
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new DoublePoint(getName(), fieldValue.doubleValue());
  }

  @Override
  protected LoadedDocValues<?> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleDouble(docValues);
  }

  @Override
  protected LoadedDocValues<?> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedDoubles(docValues);
  }

  @Override
  protected DoubleValuesSource getBindingSource() {
    return DoubleValuesSource.fromDoubleField(getName());
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.DOUBLE;
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
  }

  @Override
  public String getType() {
    return "DOUBLE";
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    double lower =
        rangeQuery.getLower().isEmpty()
            ? Double.MIN_VALUE
            : Double.parseDouble(rangeQuery.getLower());
    double upper =
        rangeQuery.getUpper().isEmpty()
            ? Double.MAX_VALUE
            : Double.parseDouble(rangeQuery.getUpper());
    ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

    Query pointQuery = DoublePoint.newRangeQuery(rangeQuery.getField(), lower, upper);

    if (!hasDocValues()) {
      return pointQuery;
    }

    Query dvQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(
            rangeQuery.getField(),
            NumericUtils.doubleToSortableLong(lower),
            NumericUtils.doubleToSortableLong(upper));
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, double lower, double upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  public Query getTermQuery(TermQuery termQuery) {
    return DoublePoint.newExactQuery(getName(), termQuery.getDoubleValue());
  }

  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    return DoublePoint.newSetQuery(getName(), termInSetQuery.getDoubleTerms().getTermsList());
  }
}
