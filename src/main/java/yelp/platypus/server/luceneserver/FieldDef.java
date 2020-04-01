/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package yelp.platypus.server.luceneserver;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.CloseableThreadLocal;

import java.io.Closeable;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Defines the type of one field.
 */
public class FieldDef implements Closeable {
    /**
     * Field name.
     */
    public final String name;

    /**
     * {@link FieldType}, used during indexing.
     */
    public final FieldType fieldType;

    /**
     * {@link FieldType} minus doc values, used during
     * indexing because we separately add the doc values field
     * and then the "normal" field.
     */
    public final FieldType fieldTypeNoDV;

    public enum FieldValueType {ATOM, TEXT, BOOLEAN, LONG, INT, DOUBLE, FLOAT, LAT_LON, DATE_TIME, VIRTUAL}

    public enum FacetValueType {NO_FACETS, FLAT, HIERARCHY, NUMERIC_RANGE, SORTED_SET_DOC_VALUES}

    /**
     * Value type (atom, text, boolean, etc.).
     */
    public final FieldValueType valueType;

    // nocommit use enum:
    /**
     * Facet type (no, flat, hierarchical, numericRange, sortedSetDocValues).
     */
    public final FacetValueType faceted;

    /**
     * Postings format (codec).
     */
    public final String postingsFormat;

    /**
     * Non-null for date time fields
     */
    public final String dateTimeFormat;

    /**
     * Doc values format (codec).
     */
    public final String docValuesFormat;

    /**
     * True if the field is multi valued.
     */
    public final boolean multiValued;

    /**
     * {@link Similarity} to use during indexing searching.
     */
    public final Similarity sim;

    /**
     * Index-time {@link Analyzer}.
     */
    public final Analyzer indexAnalyzer;

    /**
     * Search-time {@link Analyzer}.
     */
    public final Analyzer searchAnalyzer;

    /**
     * True if the field will be highlighted.
     */
    public final boolean highlighted;

    /**
     * Only set for a virtual field (expression).
     */
    public final DoubleValuesSource valueSource;

    /**
     * True if this field is indexed as a dimensional point
     */
    public final boolean usePoints;

    public static final class DateTimeParser {
        public final SimpleDateFormat parser;

        public final ParsePosition position;

        public DateTimeParser(String format) {
            parser = new SimpleDateFormat(format, Locale.ROOT);
            parser.setTimeZone(TimeZone.getTimeZone("UTC"));
            position = new ParsePosition(0);
        }
    }

    public final CloseableThreadLocal<DateTimeParser> dateTimeParsers;

    /**
     * Sole constructor.
     */
    public FieldDef(String name, FieldType fieldType, FieldValueType valueType, FacetValueType faceted,
                    String postingsFormat, String docValuesFormat, boolean multiValued, boolean usePoints,
                    Similarity sim, Analyzer indexAnalyzer, Analyzer searchAnalyzer, boolean highlighted,
                    DoubleValuesSource valueSource, String dateTimeFormat) {
        this.name = name;
        this.fieldType = fieldType;
        if (fieldType != null) {
            fieldType.freeze();
        }
        this.valueType = valueType;
        if (valueType == FieldValueType.DATE_TIME) {
            dateTimeParsers = new CloseableThreadLocal<>();
        } else {
            dateTimeParsers = null;
        }
        this.faceted = faceted;
        this.postingsFormat = postingsFormat;
        this.docValuesFormat = docValuesFormat;
        this.multiValued = multiValued;
        this.usePoints = usePoints;
        this.sim = sim;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
        this.highlighted = highlighted;
        this.dateTimeFormat = dateTimeFormat;
        // nocommit messy:
        if (fieldType != null) {
            fieldTypeNoDV = new FieldType(fieldType);
            fieldTypeNoDV.setDocValuesType(DocValuesType.NONE);
            fieldTypeNoDV.freeze();
        } else {
            fieldTypeNoDV = null;
        }
        this.valueSource = valueSource;
    }

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

    @Override
    public void close() {
        if (dateTimeParsers != null) {
            dateTimeParsers.close();
        }
    }
}
