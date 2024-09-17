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

import static com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator.hasAnalyzer;

import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.RangeQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/** Field class for 'DATE_TIME' field type. */
public class DateTimeFieldDef extends IndexableFieldDef implements Sortable, RangeQueryable {
  private static final String EPOCH_MILLIS = "epoch_millis";
  private static final String STRICT_DATE_OPTIONAL_TIME = "strict_date_optional_time";

  private final DateTimeFormatter dateTimeFormatter;
  private final String dateTimeFormat;

  private static DateTimeFormatter createDateTimeFormatter(String dateTimeFormat) {
    if (dateTimeFormat.equals(EPOCH_MILLIS)) {
      return null;
    } else if (dateTimeFormat.equals(STRICT_DATE_OPTIONAL_TIME)) {
      /**
       * This is a replication of {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} with time optional.
       */
      return new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .appendLiteral('T')
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalEnd()
          // Set default time to make sure date-only format is parsed properly as LocalDateTime
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .toFormatter()
          .withChronology(IsoChronology.INSTANCE)
          .withResolverStyle(ResolverStyle.STRICT);
    } else {
      return DateTimeFormatter.ofPattern(dateTimeFormat);
    }
  }

  public DateTimeFieldDef(String name, Field requestField) {
    super(name, requestField);
    dateTimeFormat = requestField.getDateTimeFormat();
    dateTimeFormatter = createDateTimeFormatter(dateTimeFormat);
  }

  @Override
  public SortField getSortField(SortType type) {
    if (!hasDocValues()) {
      throw new IllegalStateException("Doc values are required for sorted fields");
    }
    if (isMultiValue()) {
      throw new IllegalStateException("DATE_TIME does not support sort for multi value field");
    }
    SortField sortField = new SortField(getName(), SortField.Type.LONG, type.getReverse());
    boolean missingLast = type.getMissingLat();
    sortField.setMissingValue(missingLast ? Long.MAX_VALUE : Long.MIN_VALUE);
    return sortField;
  }

  @Override
  public Object parseLastValue(String value) {
    return Long.valueOf(value);
  }

  @Override
  public Query getRangeQuery(RangeQuery rangeQuery) {
    long lower =
        rangeQuery.getLower().isEmpty()
            ? Long.MIN_VALUE
            : convertDateStringToMillis(rangeQuery.getLower());
    long upper =
        rangeQuery.getUpper().isEmpty()
            ? Long.MAX_VALUE
            : convertDateStringToMillis(rangeQuery.getUpper());

    if (rangeQuery.getLowerExclusive()) {
      lower = Math.addExact(lower, 1);
    }
    if (rangeQuery.getUpperExclusive()) {
      upper = Math.addExact(upper, -1);
    }
    ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

    Query pointQuery = LongPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

    if (!hasDocValues()) {
      return pointQuery;
    }

    Query dvQuery =
        SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(), lower, upper);
    return new IndexOrDocValuesQuery(pointQuery, dvQuery);
  }

  private long convertDateStringToMillis(String dateString) {
    if (dateTimeFormat.equals(EPOCH_MILLIS)) {
      return Long.parseLong(dateString);
    }
    return LocalDateTime.parse(dateString, dateTimeFormatter)
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli();
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, long lower, long upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException("no analyzer allowed on datetime field");
    }

    // make sure the format is valid:
    try {
      String dateTimeFormat = requestField.getDateTimeFormat();
      if (!dateTimeFormat.equals(EPOCH_MILLIS)
          && !dateTimeFormat.equals(STRICT_DATE_OPTIONAL_TIME)) {
        DateTimeFormatter.ofPattern(dateTimeFormat);
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("dateTimeFormat could not parse pattern", iae);
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues()) {
      if (requestField.getMultiValued()) {
        return DocValuesType.SORTED_NUMERIC;
      } else {
        return DocValuesType.NUMERIC;
      }
    }
    return DocValuesType.NONE;
  }

  @Override
  protected FacetValueType parseFacetValueType(Field requestField) {
    FacetType facetType = requestField.getFacet();
    if (facetType.equals(FacetType.HIERARCHY)) {
      if (requestField.getStore()) {
        throw new IllegalArgumentException("facet=hierarchy fields cannot have store=true");
      }
      return FacetValueType.HIERARCHY;
    } else if (facetType.equals(FacetType.NUMERIC_RANGE)) {
      if (!requestField.getSearch()) {
        throw new IllegalArgumentException("facet=numericRange fields must have search=true");
      }
      return FacetValueType.NUMERIC_RANGE;
    } else if (facetType.equals(FacetType.SORTED_SET_DOC_VALUES)) {
      throw new IllegalArgumentException(
          "facet=SORTED_SET_DOC_VALUES can work only for TEXT fields");
    } else if (facetType.equals(FacetType.FLAT)) {
      return FacetValueType.FLAT;
    }
    return FacetValueType.NO_FACETS;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    }

    for (String fieldStr : fieldValues) {
      long indexValue = getTimeToIndex(fieldStr);
      if (hasDocValues()) {
        if (docValuesType == DocValuesType.NUMERIC) {
          document.add(new NumericDocValuesField(getName(), indexValue));
        } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
          document.add(new SortedNumericDocValuesField(getName(), indexValue));
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported doc value type %s for field %s", docValuesType, this.getName()));
        }
      }
      if (isSearchable()) {
        document.add(new LongPoint(getName(), indexValue));
      }
      if (isStored()) {
        document.add(new FieldWithData(getName(), fieldType, indexValue));
      }

      addFacet(document, indexValue);
    }
  }

  private void addFacet(Document document, long value) {
    if (facetValueType == FacetValueType.HIERARCHY || facetValueType == FacetValueType.FLAT) {
      String facetValue = String.valueOf(value);
      document.add(new FacetField(getName(), facetValue));
    }
  }

  private long getTimeToIndex(String dateString) {
    if (getDateTimeFormat().equals(EPOCH_MILLIS)) {
      return getTimeFromEpochMillisString(dateString);
    } else {
      return getTimeFromDateTimeString(dateString);
    }
  }

  private long getTimeFromDateTimeString(String dateString) {
    try {
      return LocalDateTime.parse(dateString, dateTimeFormatter)
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format(
              "%s could not parse %s as date_time with format %s",
              getName(), dateString, dateTimeFormat),
          e);
    }
  }

  private long getTimeFromEpochMillisString(String epochMillisString) {
    String format =
        String.format(
            "%s could not parse %s as date_time with format %s",
            getName(), epochMillisString, dateTimeFormat);
    try {
      long epochMillis = Long.parseLong(epochMillisString);
      return epochMillis;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(format, e);
    }
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.NUMERIC) {
      NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), getName());
      return new LoadedDocValues.SingleDateTime(numericDocValues);
    } else if (docValuesType == DocValuesType.SORTED_NUMERIC) {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(context.reader(), getName());
      return new LoadedDocValues.SortedDateTimes(sortedNumericDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(value.getLongValue()).build();
  }

  @Override
  public String getType() {
    return "DATE_TIME";
  }

  /**
   * Get the format used to parse date time string.
   *
   * @return date time format string
   */
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  /**
   * Format a epoch millisecond into the field's default formatted string.
   *
   * @return The formatted string representation of the input
   */
  public String formatEpochMillis(long value) {
    return dateTimeFormatter.format(Instant.ofEpochMilli(value));
  }
}
