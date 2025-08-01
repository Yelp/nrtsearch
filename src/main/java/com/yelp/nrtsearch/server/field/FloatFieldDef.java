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
package com.yelp.nrtsearch.server.field;

import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongToDoubleFunction;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.NumericUtils;

/** Field class for 'FLOAT' field type. */
public class FloatFieldDef extends NumberFieldDef<Float> {

  public FloatFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    this(name, requestField, context, null);
  }

  /**
   * Constructor for creating an instance of this field based on a previous instance. This is used
   * when updating field properties.
   *
   * @param name name of the field
   * @param requestField the field definition from the request
   * @param context context for creating the field definition
   * @param previousField the previous instance of this field definition, or null if there is none
   */
  protected FloatFieldDef(
      String name,
      Field requestField,
      FieldDefCreator.FieldDefCreatorContext context,
      FloatFieldDef previousField) {
    super(name, requestField, FLOAT_PARSER, context, Float.class, previousField);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new NumericDocValuesField(getName(), SORTED_FLOAT_ENCODER.applyAsLong(fieldValue));
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
  protected LoadedDocValues<Float> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleFloat(docValues);
  }

  @Override
  protected LoadedDocValues<Float> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedFloats(docValues);
  }

  @Override
  protected LongToDoubleFunction getBindingDecoder() {
    return BindingValuesSources.SORTED_FLOAT_DECODER;
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.FLOAT;
  }

  @Override
  public Object parseLastValue(String value) {
    return Float.valueOf(value);
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    return SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(value.getFloatValue()).build();
  }

  @Override
  public String getType() {
    return "FLOAT";
  }

  @Override
  public FieldDef createUpdatedFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    return new FloatFieldDef(name, requestField, context, this);
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    verifySearchableOrDocValues("Range query");
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

    Query pointQuery = null;
    Query dvQuery = null;
    if (isSearchable()) {
      pointQuery = FloatPoint.newRangeQuery(rangeQuery.getField(), lower, upper);
      if (!hasDocValues()) {
        return pointQuery;
      }
    }
    if (hasDocValues()) {
      dvQuery =
          SortedNumericDocValuesField.newSlowRangeQuery(
              rangeQuery.getField(),
              NumericUtils.floatToSortableInt(lower),
              NumericUtils.floatToSortableInt(upper));
      if (!isSearchable()) {
        return dvQuery;
      }
    }
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
    verifySearchable("Term query");
    return FloatPoint.newExactQuery(getName(), floatValue);
  }

  @Override
  public Query getTermInSetQueryFromFloatValues(List<Float> floatValues) {
    verifySearchable("Term in set query");
    return FloatPoint.newSetQuery(getName(), floatValues);
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    return getTermQueryFromFloatValue(Float.parseFloat(textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    List<Float> floatTerms = new ArrayList<>(textValues.size());
    textValues.forEach((s) -> floatTerms.add(Float.parseFloat(s)));
    return getTermInSetQueryFromFloatValues(floatTerms);
  }
}
