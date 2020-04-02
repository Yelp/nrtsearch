/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.RangeQuery;

import com.google.common.base.Strings;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Builds a range query for INT, LONG, FLOAT, DOUBLE and DATE_TIME types. Point based queries are always created, and
 * if docvalues are available for the field a docvalue query is also created and both point and docvalue queries are
 * wrapped in an {@link IndexOrDocValuesQuery} so that Lucene runs whichever query is more efficient.
 */
public class RangeQueryBuilder {
    private static final long MIN_DATE_TIME = 0;

    public Query buildRangeQuery(RangeQuery rangeQuery, IndexState state) {
        if (rangeQuery.getLower().isEmpty() && rangeQuery.getUpper().isEmpty()) {
            throw new IllegalArgumentException("Neither lower nor upper bound provided in RangeQuery: " + rangeQuery);
        }

        String fieldName = rangeQuery.getField();
        FieldDef field = state.getField(fieldName);
        FieldDef.FieldValueType fieldValueType = field.valueType;
        boolean hasDocValues = !Strings.isNullOrEmpty(field.docValuesFormat);

        switch (fieldValueType) {
            case INT: return getIntRangeQuery(rangeQuery, hasDocValues);
            case LONG: return getLongRangeQuery(rangeQuery, hasDocValues);
            case FLOAT: return getFloatRangeQuery(rangeQuery, hasDocValues);
            case DOUBLE: return getDoubleRangeQuery(rangeQuery, hasDocValues);
            case DATE_TIME: return getDateRangeQuery(rangeQuery, field, hasDocValues);
            default: throw new UnsupportedOperationException("Range queries are not supported for field type: " + fieldValueType);
        }
    }

    private Query getIntRangeQuery(RangeQuery rangeQuery, boolean hasDocValues) {
        int lower = rangeQuery.getLower().isEmpty() ? Integer.MIN_VALUE : Integer.parseInt(rangeQuery.getLower());
        int upper = rangeQuery.getUpper().isEmpty() ? Integer.MAX_VALUE : Integer.parseInt(rangeQuery.getUpper());

        Query pointQuery = IntPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

        if (!hasDocValues) {
            return pointQuery;
        }

        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(), lower, upper);
        return new IndexOrDocValuesQuery(pointQuery, dvQuery);
    }

    private Query getLongRangeQuery(RangeQuery rangeQuery, boolean hasDocValues) {
        long lower = rangeQuery.getLower().isEmpty() ? Long.MIN_VALUE : Long.parseLong(rangeQuery.getLower());
        long upper = rangeQuery.getUpper().isEmpty() ? Long.MAX_VALUE : Long.parseLong(rangeQuery.getUpper());
        ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

        return getLongRangeQuery(rangeQuery.getField(), lower, upper, hasDocValues);
    }

    private Query getLongRangeQuery(String field, long lower, long upper, boolean hasDocValues) {
        Query pointQuery = LongPoint.newRangeQuery(field, lower, upper);

        if (!hasDocValues) {
            return pointQuery;
        }

        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper);
        return new IndexOrDocValuesQuery(pointQuery, dvQuery);
    }

    private Query getFloatRangeQuery(RangeQuery rangeQuery, boolean hasDocValues) {
        float lower = rangeQuery.getLower().isEmpty() ? Float.MIN_VALUE : Float.parseFloat(rangeQuery.getLower());
        float upper = rangeQuery.getUpper().isEmpty() ? Float.MAX_VALUE : Float.parseFloat(rangeQuery.getUpper());
        ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

        Query pointQuery = FloatPoint.newRangeQuery(rangeQuery.getField(), lower, upper);

        if (!hasDocValues) {
            return pointQuery;
        }

        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(),
                NumericUtils.floatToSortableInt(lower),
                NumericUtils.floatToSortableInt(upper));
        return new IndexOrDocValuesQuery(pointQuery, dvQuery);
    }

    private Query getDoubleRangeQuery(RangeQuery rangeQuery, boolean hasDocValues) {
        double lower = rangeQuery.getLower().isEmpty() ? Double.MIN_VALUE : Double.parseDouble(rangeQuery.getLower());
        double upper = rangeQuery.getUpper().isEmpty() ? Double.MAX_VALUE : Double.parseDouble(rangeQuery.getUpper());
        ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

        Query pointQuery = DoublePoint.newRangeQuery(rangeQuery.getField(), lower, upper);

        if (!hasDocValues) {
            return pointQuery;
        }

        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(rangeQuery.getField(),
                NumericUtils.doubleToSortableLong(lower),
                NumericUtils.doubleToSortableLong(upper));
        return new IndexOrDocValuesQuery(pointQuery, dvQuery);
    }

    private Query getDateRangeQuery(RangeQuery rangeQuery, FieldDef fd, boolean hasDocValues) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(fd.dateTimeFormat);

        long lower = rangeQuery.getLower().isEmpty() ? MIN_DATE_TIME : convertDateStringToMillis(rangeQuery.getLower(), dateTimeFormatter);
        long upper = rangeQuery.getUpper().isEmpty() ? Long.MAX_VALUE : convertDateStringToMillis(rangeQuery.getUpper(), dateTimeFormatter);
        ensureUpperIsMoreThanLower(rangeQuery, lower, upper);

        return getLongRangeQuery(rangeQuery.getField(), lower, upper, hasDocValues);
    }

    private void ensureUpperIsMoreThanLower(RangeQuery rangeQuery, double lower, double upper) {
        if (lower > upper) {
            throw new IllegalArgumentException("Lower value is higher than upper value for RangeQuery: " + rangeQuery);
        }
    }

    private static long convertDateStringToMillis(String dateString, DateTimeFormatter dateTimeFormatter) {
        return LocalDateTime.parse(dateString, dateTimeFormatter).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
