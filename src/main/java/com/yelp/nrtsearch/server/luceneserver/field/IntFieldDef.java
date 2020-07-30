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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'INT' field type. */
public class IntFieldDef extends NumberFieldDef {

  public IntFieldDef(String name, Field requestField) {
    super(name, requestField, INT_PARSER);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new NumericDocValuesField(getName(), fieldValue.intValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(getName(), fieldValue.intValue());
    }
    throw new IllegalStateException("Unsupported doc value type: " + docValuesType);
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new IntPoint(getName(), fieldValue.intValue());
  }

  @Override
  protected LoadedDocValues<?> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleInteger(docValues);
  }

  @Override
  protected LoadedDocValues<?> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedIntegers(docValues);
  }

  @Override
  protected DoubleValuesSource getBindingSource() {
    return DoubleValuesSource.fromIntField(getName());
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.INT;
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Integer.MAX_VALUE : Integer.MIN_VALUE;
  }

  @Override
  public String getType() {
    return "INT";
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    int lower =
        rangeQuery.getLower().isEmpty()
            ? Integer.MIN_VALUE
            : Integer.parseInt(rangeQuery.getLower());
    int upper =
        rangeQuery.getUpper().isEmpty()
            ? Integer.MAX_VALUE
            : Integer.parseInt(rangeQuery.getUpper());

    Query pointQuery = IntPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

    if (!hasDocValues()) {
      return pointQuery;
    }

    Query dvQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(), lower, upper);
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  @Override
  public Query getTermQuery(TermQuery termQuery) {
    return IntPoint.newExactQuery(getName(), termQuery.getIntValue());
  }

  @Override
  public Query getTermInSetQuery(TermInSetQuery termInSetQuery) {
    return IntPoint.newSetQuery(getName(), termInSetQuery.getIntTerms().getTermsList());
  }
}
