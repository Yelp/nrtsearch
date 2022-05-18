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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongToDoubleFunction;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.NumericUtils;

/** Field class for 'FLOAT' field type. */
public class FloatFieldDef extends NumberFieldDef {

  public FloatFieldDef(String name, Field requestField) {
    super(name, requestField, FLOAT_PARSER);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new FloatDocValuesField(getName(), fieldValue.floatValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(
          getName(), SORTED_FLOAT_ENCODER.applyAsLong(fieldValue));
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new FloatPoint(getName(), fieldValue.floatValue());
  }

  @Override
  protected LoadedDocValues<?> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleFloat(docValues);
  }

  @Override
  protected LoadedDocValues<?> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedFloats(docValues);
  }

  @Override
  protected LongToDoubleFunction getBindingDecoder() {
    if (isMultiValue()) {
      return BindingValuesSources.SORTED_FLOAT_DECODER;
    } else {
      return BindingValuesSources.FLOAT_DECODER;
    }
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.FLOAT;
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
  }

  @Override
  public String getType() {
    return "FLOAT";
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    float lower =
        rangeQuery.getLower().isEmpty()
            ? Float.NEGATIVE_INFINITY
            : Float.parseFloat(rangeQuery.getLower());
    float upper =
        rangeQuery.getUpper().isEmpty()
            ? Float.POSITIVE_INFINITY
            : Float.parseFloat(rangeQuery.getUpper());

    if (rangeQuery.getLowerExclusive()) {
      lower = FloatPoint.nextUp(lower);
    }
    if (rangeQuery.getUpperExclusive()) {
      upper = FloatPoint.nextDown(upper);
    }
    ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

    Query pointQuery = FloatPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

    if (!hasDocValues()) {
      return pointQuery;
    }

    Query dvQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(
            rangeQuery.getField(),
            NumericUtils.floatToSortableInt(lower),
            NumericUtils.floatToSortableInt(upper));
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, float lower, float upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  public Query getTermQueryFromFloatValue(float floatValue) {
    return FloatPoint.newExactQuery(getName(), floatValue);
  }

  @Override
  public Query getTermInSetQueryFromFloatValues(List<Float> floatValues) {
    return FloatPoint.newSetQuery(getName(), floatValues);
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    return FloatPoint.newExactQuery(getName(), Float.parseFloat(textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    List<Float> floatTerms = new ArrayList(textValues.size());
    textValues.forEach((s) -> floatTerms.add(Float.parseFloat(s)));
    return FloatPoint.newSetQuery(getName(), floatTerms);
  }
}
