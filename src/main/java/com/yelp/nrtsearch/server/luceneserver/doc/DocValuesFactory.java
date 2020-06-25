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
package com.yelp.nrtsearch.server.luceneserver.doc;

import com.yelp.nrtsearch.server.luceneserver.FieldDef;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Static utility class that can create the appropriate {@link LoadedDocValues} implementation for a
 * given {@link FieldDef}. The implementation is determined using the field's doc value type and
 * value type (when required).
 */
public class DocValuesFactory {
  // This class can have no instance
  private DocValuesFactory() {}

  /**
   * Get the correct {@link LoadedDocValues} implementation for the given {@link FieldDef}, and
   * bound to the provided lucene segment context.
   *
   * @param field definition of field to load values from
   * @param context lucene segment context
   * @return {@link LoadedDocValues} for the given field
   * @throws IOException if there is a problem getting the lucene doc values accessor
   */
  public static LoadedDocValues<?> getDocValues(FieldDef field, LeafReaderContext context)
      throws IOException {
    Objects.requireNonNull(field);
    switch (field.fieldType.docValuesType()) {
      case NUMERIC:
        return getNumericForValueType(field, context, field.valueType);
      case SORTED_NUMERIC:
        return getSortedNumericForValueType(field, context, field.valueType);
      case BINARY:
        BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), field.name);
        // Currently, fields of the BINARY type only contain single strings.
        // If we decide to add a pure binary type, we will need to rethink this.
        return new LoadedDocValues.SingleString(binaryDocValues);
      case SORTED:
        SortedDocValues sortedDocValues = DocValues.getSorted(context.reader(), field.name);
        return new LoadedDocValues.SingleString(sortedDocValues);
      case SORTED_SET:
        SortedSetDocValues sortedSetDocValues =
            DocValues.getSortedSet(context.reader(), field.name);
        return new LoadedDocValues.SortedStrings(sortedSetDocValues);
      default:
        throw new IllegalArgumentException(
            "Unable to load doc values for field: "
                + field.name
                + ", doc value type: "
                + field.fieldType.docValuesType());
    }
  }

  private static LoadedDocValues<?> getNumericForValueType(
      FieldDef field, LeafReaderContext context, FieldDef.FieldValueType valueType)
      throws IOException {
    NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), field.name);
    switch (valueType) {
      case BOOLEAN:
        return new LoadedDocValues.SingleBoolean(numericDocValues);
      case INT:
        return new LoadedDocValues.SingleInteger(numericDocValues);
      case LONG:
        return new LoadedDocValues.SingleLong(numericDocValues);
      case FLOAT:
        return new LoadedDocValues.SingleFloat(numericDocValues);
      case DOUBLE:
        return new LoadedDocValues.SingleDouble(numericDocValues);
      case DATE_TIME:
        return new LoadedDocValues.SingleDateTime(numericDocValues);
      default:
        throw new IllegalArgumentException(
            "Unable to create doc value loader for field: " + field.name + ", type: " + valueType);
    }
  }

  private static LoadedDocValues<?> getSortedNumericForValueType(
      FieldDef field, LeafReaderContext context, FieldDef.FieldValueType valueType)
      throws IOException {
    SortedNumericDocValues sortedNumericDocValues =
        DocValues.getSortedNumeric(context.reader(), field.name);
    switch (valueType) {
      case BOOLEAN:
        return new LoadedDocValues.SortedBooleans(sortedNumericDocValues);
      case INT:
        return new LoadedDocValues.SortedIntegers(sortedNumericDocValues);
      case LONG:
        return new LoadedDocValues.SortedLongs(sortedNumericDocValues);
      case FLOAT:
        return new LoadedDocValues.SortedFloats(sortedNumericDocValues);
      case DOUBLE:
        return new LoadedDocValues.SortedDoubles(sortedNumericDocValues);
      case LAT_LON:
        return new LoadedDocValues.Locations(sortedNumericDocValues);
      case DATE_TIME:
        return new LoadedDocValues.SortedDateTimes(sortedNumericDocValues);
      default:
        throw new IllegalArgumentException(
            "Unable to create doc value loader for field: " + field.name + ", type: " + valueType);
    }
  }
}
