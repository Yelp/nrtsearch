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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'INT' field type. */
public class IntFieldDef extends NumberFieldDef<Integer> {

  public IntFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    super(name, requestField, INT_PARSER, context, Integer.class);
  }

  @Override
  protected org.apache.lucene.document.Field getDocValueField(Number fieldValue) {
    if (docValuesType == DocValuesType.NUMERIC) {
      return new NumericDocValuesField(getName(), fieldValue.intValue());
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      return new SortedNumericDocValuesField(getName(), fieldValue.intValue());
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  protected org.apache.lucene.document.Field getPointField(Number fieldValue) {
    return new IntPoint(getName(), fieldValue.intValue());
  }

  @Override
  protected LoadedDocValues<Integer> getNumericDocValues(NumericDocValues docValues) {
    return new LoadedDocValues.SingleInteger(docValues);
  }

  @Override
  protected LoadedDocValues<Integer> getSortedNumericDocValues(SortedNumericDocValues docValues) {
    return new LoadedDocValues.SortedIntegers(docValues);
  }

  @Override
  protected LongToDoubleFunction getBindingDecoder() {
    return BindingValuesSources.INT_DECODER;
  }

  @Override
  protected SortField.Type getSortFieldType() {
    return SortField.Type.INT;
  }

  @Override
  public Object parseLastValue(String value) {
    return Integer.valueOf(value);
  }

  @Override
  protected Number getSortMissingValue(boolean missingLast) {
    return missingLast ? Integer.MAX_VALUE : Integer.MIN_VALUE;
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    return SearchResponse.Hit.FieldValue.newBuilder().setIntValue(value.getIntValue()).build();
  }

  @Override
  public String getType() {
    return "INT";
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    verifySearchableOrDocValues("Range query");
    int lower =
        rangeQuery.getLower().isEmpty()
            ? Integer.MIN_VALUE
            : Integer.parseInt(rangeQuery.getLower());
    int upper =
        rangeQuery.getUpper().isEmpty()
            ? Integer.MAX_VALUE
            : Integer.parseInt(rangeQuery.getUpper());

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
      pointQuery = IntPoint.newRangeQuery(rangeQuery.getField(), lower, upper);
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

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, int lower, int upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  public Query getTermQueryFromIntValue(int intValue) {
    verifySearchable("Term query");
    return IntPoint.newExactQuery(getName(), intValue);
  }

  @Override
  public Query getTermInSetQueryFromIntValues(List<Integer> intValues) {
    verifySearchable("Term in set query");
    return IntPoint.newSetQuery(getName(), intValues);
  }

  @Override
  public Query getTermQueryFromTextValue(String textValue) {
    return getTermQueryFromIntValue(Integer.parseInt(textValue));
  }

  @Override
  public Query getTermInSetQueryFromTextValues(List<String> textValues) {
    List<Integer> intTerms = new ArrayList<>(textValues.size());
    textValues.forEach((s) -> intTerms.add(Integer.parseInt(s)));
    return getTermInSetQueryFromIntValues(intTerms);
  }

  @Override
  protected Number parseNumberString(String numberString) {
    // Integer::valueOf will fail for cases like 1.0
    // GSON will convert all numbers to float during deserialization
    // for cases like 1.0, use double parser to parse the value
    if (numberString.indexOf('.') == -1) {
      return super.parseNumberString(numberString);
    } else {
      return DOUBLE_PARSER.apply(numberString).intValue();
    }
  }
}
