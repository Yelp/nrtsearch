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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'LONG' field type. */
public class LongFieldDef extends NumberFieldDef<Long> {

  public LongFieldDef(
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
  protected LongFieldDef(
      String name,
      Field requestField,
      FieldDefCreator.FieldDefCreatorContext context,
      LongFieldDef previousField) {
    super(name, requestField, LONG_PARSER, context, Long.class, previousField);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new NumericDocValuesField(getName(), fieldValue.longValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(getName(), fieldValue.longValue());
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new LongPoint(getName(), fieldValue.longValue());
  }

  @Override
  protected LoadedDocValues<Long> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleLong(docValues);
  }

  @Override
  protected LoadedDocValues<Long> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedLongs(docValues);
  }

  @Override
  protected LongToDoubleFunction getBindingDecoder() {
    return BindingValuesSources.LONG_DECODER;
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.LONG;
  }

  @Override
  public Object parseLastValue(String value) {
    return Long.valueOf(value);
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Long.MAX_VALUE : Long.MIN_VALUE;
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(value.getLongValue()).build();
  }

  @Override
  public String getType() {
    return "LONG";
  }

  @Override
  public FieldDef createUpdatedFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    return new LongFieldDef(name, requestField, context, this);
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    verifySearchableOrDocValues("Range query");
    long lower =
        rangeQuery.getLower().isEmpty() ? Long.MIN_VALUE : Long.parseLong(rangeQuery.getLower());
    long upper =
        rangeQuery.getUpper().isEmpty() ? Long.MAX_VALUE : Long.parseLong(rangeQuery.getUpper());

    if (rangeQuery.getLowerExclusive()) {
      lower = Math.addExact(lower, 1);
    }
    if (rangeQuery.getUpperExclusive()) {
      upper = Math.addExact(upper, -1);
    }
    ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

    Query pointQuery = null;
    Query dvQuery = null;
    if (isSearchable()) {
      pointQuery = LongPoint.newRangeQuery(rangeQuery.getField(), lower, upper);
      if (!hasDocValues()) {
        return pointQuery;
      }
    }
    if (hasDocValues()) {
      dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(), lower, upper);
      if (!isSearchable()) {
        return dvQuery;
      }
    }
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, long lower, long upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  public Query getTermQueryFromLongValue(long longValue) {
    verifySearchable("Term query");
    return LongPoint.newExactQuery(getName(), longValue);
  }

  @Override
  public Query getTermInSetQueryFromLongValues(List<Long> longValues) {
    verifySearchable("Term in set query");
    return LongPoint.newSetQuery(getName(), longValues);
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    return getTermQueryFromLongValue(Long.parseLong(textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    List<Long> longTerms = new ArrayList<>(textValues.size());
    textValues.forEach((s) -> longTerms.add(Long.parseLong(s)));
    return getTermInSetQueryFromLongValues(longTerms);
  }

  protected Number parseNumberString(String numberString) {
    // Long::valueOf will fail for cases like 1.0
    // GSON will convert all numbers to float during deserialization
    // for cases like 1.0, use double parser to parse the value
    if (numberString.indexOf('.') == -1) {
      return super.parseNumberString(numberString);
    } else {
      return DOUBLE_PARSER.apply(numberString).longValue();
    }
  }
}
