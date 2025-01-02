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

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongToDoubleFunction;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.NumericUtils;

/**
 * Container class for {@link DoubleValuesSource} implementations that expose various field
 * properties to lucene {@link org.apache.lucene.expressions.Expression} scripts.
 */
public class BindingValuesSources {

  // decoders to convert the long value read from doc value fields into a double
  public static LongToDoubleFunction INT_DECODER = value -> (double) value;
  public static LongToDoubleFunction LONG_DECODER = value -> (double) value;
  public static LongToDoubleFunction SORTED_FLOAT_DECODER =
      value -> (double) NumericUtils.sortableIntToFloat((int) value);
  public static LongToDoubleFunction SORTED_DOUBLE_DECODER = NumericUtils::sortableLongToDouble;

  private BindingValuesSources() {}

  public abstract static class NumericValuesSourceBase extends DoubleValuesSource {

    final String field;

    public NumericValuesSourceBase(String field) {
      this.field = field;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(field);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NumericValuesSourceBase that = (NumericValuesSourceBase) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match(values.doubleValue(), this.toString());
      else return Explanation.noMatch(this.toString());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }
  }

  /** Value source for the length of a single valued field. */
  public static class NumericLengthValuesSource extends NumericValuesSourceBase {

    public NumericLengthValuesSource(String field) {
      super(field);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      NumericDocValues numericDocValues = DocValues.getNumeric(ctx.reader(), field);
      return new DoubleValues() {
        double length;

        @Override
        public double doubleValue() throws IOException {
          return length;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          length = numericDocValues.advanceExact(doc) ? 1 : 0;
          return true;
        }
      };
    }

    @Override
    public String toString() {
      return "length(" + field + ")";
    }
  }

  /** Value source to determine if a single valued field is empty. */
  public static class NumericEmptyValuesSource extends NumericValuesSourceBase {

    public NumericEmptyValuesSource(String field) {
      super(field);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      NumericDocValues numericDocValues = DocValues.getNumeric(ctx.reader(), field);
      return new DoubleValues() {
        double length;

        @Override
        public double doubleValue() throws IOException {
          return length;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          length = numericDocValues.advanceExact(doc) ? 0 : 1;
          return true;
        }
      };
    }

    @Override
    public String toString() {
      return "empty(" + field + ")";
    }
  }

  /** Value source for the value of a single valued field. */
  public static class NumericDecodedValuesSource extends DoubleValuesSource {

    final String field;
    final LongToDoubleFunction decoder;

    public NumericDecodedValuesSource(String field, LongToDoubleFunction decoder) {
      this.field = field;
      this.decoder = decoder;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      final NumericDocValues numericDocValues = DocValues.getNumeric(ctx.reader(), field);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return decoder.applyAsDouble(numericDocValues.longValue());
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return numericDocValues.advanceExact(target);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NumericDecodedValuesSource that = (NumericDecodedValuesSource) o;
      return Objects.equals(field, that.field) && Objects.equals(decoder, that.decoder);
    }

    @Override
    public String toString() {
      return "double(" + field + ")";
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match(values.doubleValue(), this.toString());
      else return Explanation.noMatch(this.toString());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }
  }

  public abstract static class SortedNumericValuesSourceBase extends DoubleValuesSource {

    final String field;

    public SortedNumericValuesSourceBase(String field) {
      this.field = field;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(field);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SortedNumericValuesSourceBase that = (SortedNumericValuesSourceBase) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match(values.doubleValue(), this.toString());
      else return Explanation.noMatch(this.toString());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }
  }

  /** Value source for the length of a multi valued field. */
  public static class SortedNumericLengthValuesSource extends SortedNumericValuesSourceBase {

    public SortedNumericLengthValuesSource(String field) {
      super(field);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(ctx.reader(), field);
      return new DoubleValues() {
        boolean hasValues;

        @Override
        public double doubleValue() throws IOException {
          if (hasValues) {
            return sortedNumericDocValues.docValueCount();
          }
          return 0;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          hasValues = sortedNumericDocValues.advanceExact(doc);
          return true;
        }
      };
    }

    @Override
    public String toString() {
      return "length(" + field + ")";
    }
  }

  /** Value source to determine if a multi valued field is empty. */
  public static class SortedNumericEmptyValuesSource extends SortedNumericValuesSourceBase {

    public SortedNumericEmptyValuesSource(String field) {
      super(field);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(ctx.reader(), field);
      return new DoubleValues() {
        boolean hasValues;

        @Override
        public double doubleValue() throws IOException {
          return hasValues ? 0 : 1;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          hasValues = sortedNumericDocValues.advanceExact(doc);
          return true;
        }
      };
    }

    @Override
    public String toString() {
      return "empty(" + field + ")";
    }
  }

  public abstract static class SortedNumericDecodedValuesSource extends DoubleValuesSource {

    final String field;
    final LongToDoubleFunction decoder;

    public SortedNumericDecodedValuesSource(String field, LongToDoubleFunction decoder) {
      this.field = field;
      this.decoder = decoder;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SortedNumericDecodedValuesSource that = (SortedNumericDecodedValuesSource) o;
      return Objects.equals(field, that.field) && Objects.equals(decoder, that.decoder);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation)
        throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match(values.doubleValue(), this.toString());
      else return Explanation.noMatch(this.toString());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }
  }

  /** Value source to return the minimum (first) value of a multi valued field. */
  public static class SortedNumericMinValuesSource extends SortedNumericDecodedValuesSource {

    public SortedNumericMinValuesSource(String field, LongToDoubleFunction decoder) {
      super(field, decoder);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      SortedNumericDocValues sortedNumericDocValues =
          DocValues.getSortedNumeric(ctx.reader(), field);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return decoder.applyAsDouble(sortedNumericDocValues.nextValue());
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return sortedNumericDocValues.advanceExact(target);
        }
      };
    }

    @Override
    public String toString() {
      return "minDouble(" + field + ")";
    }
  }
}
