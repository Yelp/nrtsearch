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

import com.yelp.nrtsearch.server.grpc.FacetType;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.properties.RangeQueryable;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.CloseableThreadLocal;

/** Field class for 'DATE_TIME' field type. */
public class DateTimeFieldDef extends IndexableFieldDef implements Sortable, RangeQueryable {
  private static final String EPOCH_MILLIS = "epoch_millis";

  private final CloseableThreadLocal<DateTimeParser> dateTimeParsers;
  private final String dateTimeFormat;

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
  public Query getRangeQuery(RangeQuery rangeQuery) {
    String dateTimeFormat = getDateTimeFormat();

    long lower =
        rangeQuery.getLower().isEmpty()
            ? Long.MIN_VALUE
            : convertDateStringToMillis(rangeQuery.getLower(), dateTimeFormat);
    long upper =
        rangeQuery.getUpper().isEmpty()
            ? Long.MAX_VALUE
            : convertDateStringToMillis(rangeQuery.getUpper(), dateTimeFormat);

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

  private static long convertDateStringToMillis(String dateString, String dateTimeFormat) {
    if (dateTimeFormat.equals(EPOCH_MILLIS)) {
      return Long.parseLong(dateString);
    } else {
      DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
      return LocalDateTime.parse(dateString, dateTimeFormatter)
          .toInstant(ZoneOffset.UTC)
          .toEpochMilli();
    }
  }

  private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, long lower, long upper) {
    if (lower > upper) {
      throw new IllegalArgumentException(
          "Lower value is higher than upper value for RangeQuery: " + rangeQuery);
    }
  }

  public static final class DateTimeParser {
    public final SimpleDateFormat parser;

    public final ParsePosition position;

    public DateTimeParser(String format) {
      parser = new SimpleDateFormat(format, Locale.ROOT);
      parser.setTimeZone(TimeZone.getTimeZone("UTC"));
      position = new ParsePosition(0);
    }
  }

  public DateTimeFieldDef(String name, Field requestField) {
    super(name, requestField);
    dateTimeFormat = requestField.getDateTimeFormat();
    dateTimeParsers = new CloseableThreadLocal<>();
  }

  @Override
  protected void validateRequest(Field requestField) {
    super.validateRequest(requestField);

    if (requestField.getHighlight()) {
      throw new IllegalArgumentException(
          String.format(
              "field: %s cannot have highlight=true. only type=text or type=atom fields can have highlight=true",
              getName()));
    }

    if (requestField.getGroup()) {
      throw new IllegalArgumentException("cannot group on datatime field");
    }

    if (hasAnalyzer(requestField)) {
      throw new IllegalArgumentException("no analyzer allowed on datetime field");
    }

    // make sure the format is valid:
    try {
      String dateTimeFormat = requestField.getDateTimeFormat();
      if (!dateTimeFormat.equals(EPOCH_MILLIS)) {
        new SimpleDateFormat(dateTimeFormat);
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("dateTimeFormat could not parse pattern", iae);
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues() || requestField.getSort()) {
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
    DateTimeParser parser = getDateTimeParser();
    parser.position.setIndex(0);
    Date date = parser.parser.parse(dateString, parser.position);
    String format =
        String.format(
            "%s could not parse %s as date_time with format %s",
            getName(), dateString, dateTimeFormat);
    if (parser.position.getErrorIndex() != -1) {
      throw new IllegalArgumentException(format);
    }
    if (parser.position.getIndex() != dateString.length()) {
      throw new IllegalArgumentException(format);
    }
    return date.getTime();
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
  public String getType() {
    return "DATE_TIME";
  }

  @Override
  public void close() {
    if (dateTimeParsers != null) {
      dateTimeParsers.close();
    }
  }

  /**
   * Get a thread local parser for string based date time for this field.
   *
   * @return date time parser for strings
   */
  public DateTimeParser getDateTimeParser() {
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    calendar.setLenient(false);

    DateTimeParser parser = dateTimeParsers.get();
    if (parser == null) {
      parser = new DateTimeParser(dateTimeFormat);
      dateTimeParsers.set(parser);
    }

    return parser;
  }

  /**
   * Get the format used to parse date time string.
   *
   * @return date time format string
   */
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }
}
