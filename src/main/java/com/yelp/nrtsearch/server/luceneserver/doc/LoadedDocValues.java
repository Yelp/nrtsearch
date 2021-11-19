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

import static org.apache.lucene.util.ArrayUtil.oversize;

import com.google.gson.Gson;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoPoint;
import java.io.IOException;
import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Container class for loading and holding doc values for a field. Abstracts loaded values as a
 * list. The various implementations of this class use the lucene segment doc values accessors to
 * load the data for that field.
 *
 * <p>These are used during field data retrieval when building a search response, and provided to
 * scripts during execution.
 *
 * <p>All implementations must define setDocId to advance the doc values accessor to the provided
 * segment document. All implementations must also define toFieldValue, which provides a {@link
 * com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue} containing the doc value data for a
 * given index in the list.
 *
 * <p>All implementations throw an IllegalStateException when trying to get a value when there are
 * no values for the field. All implementations throw an IndexOutOfBoundsException when trying to
 * access an invalid index.
 *
 * @param <T> the loaded doc values type. This could be a simple boxed primitive, or something more
 *     complex like a {@link GeoPoint}.
 */
public abstract class LoadedDocValues<T> extends AbstractList<T> {
  private static final LongFunction<Instant> DATE_DECODER = Instant::ofEpochMilli;
  private static final LongFunction<GeoPoint> GEO_POINT_DECODER =
      (longValue) ->
          new GeoPoint(
              GeoEncodingUtils.decodeLatitude((int) (longValue >> 32)),
              GeoEncodingUtils.decodeLongitude((int) longValue));

  // BytesRef decoders
  // copy the target buffer, as the original BytesRef buffer will be reused
  private static final Function<BytesRef, BytesRef> BYTES_REF_DECODER = BytesRef::deepCopyOf;
  private static final Function<BytesRef, String> STRING_DECODER = BytesRef::utf8ToString;

  // Gson decoder to deserialize string to objects
  private static final Gson gson = new Gson();

  public abstract void setDocId(int docID) throws IOException;

  public abstract SearchResponse.Hit.FieldValue toFieldValue(int index);

  public abstract static class SingleNumericValue<T> extends LoadedDocValues<T> {
    private final NumericDocValues docValues;
    private final LongFunction<T> decoder;
    private T value;

    SingleNumericValue(NumericDocValues docValues, LongFunction<T> decoder) {
      this.docValues = docValues;
      this.decoder = decoder;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = decoder.apply(docValues.longValue());
      } else {
        value = null;
      }
    }

    @Override
    public T get(int index) {
      if (value == null) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return value == null ? 0 : 1;
    }

    public T getValue() {
      return get(0);
    }
  }

  public static final class SingleBoolean extends LoadedDocValues<Boolean> {
    private final NumericDocValues docValues;
    private boolean value;
    private boolean isSet;

    public SingleBoolean(NumericDocValues docValues) {
      this.docValues = docValues;
      this.isSet = false;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = docValues.longValue() == 1;
        isSet = true;
      } else {
        isSet = false;
      }
    }

    @Override
    public Boolean get(int index) {
      return getBoolean(index);
    }

    public boolean getBoolean(int index) {
      if (!isSet) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return isSet ? 1 : 0;
    }

    public boolean getValue() {
      return getBoolean(0);
    }

    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue(getBoolean(index)).build();
    }
  }

  public static final class SingleInteger extends LoadedDocValues<Integer> {
    private final NumericDocValues docValues;
    private int value;
    private boolean isSet;

    public SingleInteger(NumericDocValues docValues) {
      this.docValues = docValues;
      this.isSet = false;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = (int) docValues.longValue();
        isSet = true;
      } else {
        isSet = false;
      }
    }

    @Override
    public Integer get(int index) {
      return getInt(index);
    }

    public int getInt(int index) {
      if (!isSet) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return isSet ? 1 : 0;
    }

    public int getValue() {
      return getInt(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setIntValue(getInt(index)).build();
    }
  }

  public static final class SingleLong extends LoadedDocValues<Long> {
    private final NumericDocValues docValues;
    private long value;
    private boolean isSet;

    public SingleLong(NumericDocValues docValues) {
      this.docValues = docValues;
      this.isSet = false;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = docValues.longValue();
        isSet = true;
      } else {
        isSet = false;
      }
    }

    @Override
    public Long get(int index) {
      return getLong(index);
    }

    public long getLong(int index) {
      if (!isSet) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return isSet ? 1 : 0;
    }

    public long getValue() {
      return getLong(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(getLong(index)).build();
    }
  }

  public static final class SingleFloat extends LoadedDocValues<Float> {
    private final NumericDocValues docValues;
    private float value;
    private boolean isSet;

    public SingleFloat(NumericDocValues docValues) {
      this.docValues = docValues;
      this.isSet = false;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = Float.intBitsToFloat((int) docValues.longValue());
        isSet = true;
      } else {
        isSet = false;
      }
    }

    @Override
    public Float get(int index) {
      return getFloat(index);
    }

    public float getFloat(int index) {
      if (!isSet) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return isSet ? 1 : 0;
    }

    public float getValue() {
      return getFloat(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(getFloat(index)).build();
    }
  }

  public static final class SingleDouble extends LoadedDocValues<Double> {
    private final NumericDocValues docValues;
    private double value;
    private boolean isSet;

    public SingleDouble(NumericDocValues docValues) {
      this.docValues = docValues;
      this.isSet = false;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = Double.longBitsToDouble(docValues.longValue());
        isSet = true;
      } else {
        isSet = false;
      }
    }

    @Override
    public Double get(int index) {
      return getDouble(index);
    }

    public double getDouble(int index) {
      if (!isSet) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return isSet ? 1 : 0;
    }

    public double getValue() {
      return getDouble(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(getDouble(index)).build();
    }
  }

  public static final class SingleDateTime extends SingleNumericValue<Instant> {
    public SingleDateTime(NumericDocValues docValues) {
      super(docValues, DATE_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      long epochMs = get(index).toEpochMilli();
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(epochMs).build();
    }
  }

  public abstract static class SortedNumericValues<T> extends LoadedDocValues<T> {
    private final SortedNumericDocValues docValues;
    private final LongFunction<T> decoder;
    private final ArrayList<T> values = new ArrayList<>();

    SortedNumericValues(SortedNumericDocValues docValues, LongFunction<T> decoder) {
      this.docValues = docValues;
      this.decoder = decoder;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      values.clear();
      if (docValues.advanceExact(docID)) {
        int count = docValues.docValueCount();
        values.ensureCapacity(count);
        for (int i = 0; i < count; ++i) {
          values.add(decoder.apply(docValues.nextValue()));
        }
      }
    }

    @Override
    public T get(int index) {
      if (values.isEmpty()) {
        throw new IllegalStateException("No doc values for document");
      }
      return values.get(index);
    }

    @Override
    public int size() {
      return values.size();
    }

    public T getValue() {
      return get(0);
    }
  }

  public static final class SortedBooleans extends LoadedDocValues<Boolean> {
    private final SortedNumericDocValues docValues;
    private boolean[] values = new boolean[0];
    private int size;

    public SortedBooleans(SortedNumericDocValues docValues) {
      this.docValues = docValues;
      this.size = 0;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        size = docValues.docValueCount();
        values = grow(values, size);
        for (int i = 0; i < size; ++i) {
          values[i] = docValues.nextValue() == 1;
        }
      } else {
        size = 0;
      }
    }

    @Override
    public Boolean get(int index) {
      return getBoolean(index);
    }

    public boolean getBoolean(int index) {
      if (size == 0) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values[index];
    }

    @Override
    public int size() {
      return size;
    }

    public boolean getValue() {
      return getBoolean(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue(getBoolean(index)).build();
    }

    // Lucene ArrayUtil does not have grow functions for boolean[], added here for support
    static boolean[] grow(boolean[] array, int minSize) {
      assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
      if (array.length < minSize) {
        return growExact(array, oversize(minSize, 1));
      } else return array;
    }

    static boolean[] growExact(boolean[] array, int newLength) {
      boolean[] copy = new boolean[newLength];
      System.arraycopy(array, 0, copy, 0, array.length);
      return copy;
    }
  }

  public static final class SortedIntegers extends LoadedDocValues<Integer> {
    private final SortedNumericDocValues docValues;
    private int[] values = new int[0];
    private int size;

    public SortedIntegers(SortedNumericDocValues docValues) {
      this.docValues = docValues;
      this.size = 0;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        size = docValues.docValueCount();
        values = ArrayUtil.grow(values, size);
        for (int i = 0; i < size; ++i) {
          values[i] = (int) docValues.nextValue();
        }
      } else {
        size = 0;
      }
    }

    @Override
    public Integer get(int index) {
      return getInt(index);
    }

    public int getInt(int index) {
      if (size == 0) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values[index];
    }

    @Override
    public int size() {
      return size;
    }

    public int getValue() {
      return getInt(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setIntValue(getInt(index)).build();
    }
  }

  public static final class SortedLongs extends LoadedDocValues<Long> {
    private final SortedNumericDocValues docValues;
    private long[] values = new long[0];
    private int size;

    public SortedLongs(SortedNumericDocValues docValues) {
      this.docValues = docValues;
      this.size = 0;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        size = docValues.docValueCount();
        values = ArrayUtil.grow(values, size);
        for (int i = 0; i < size; ++i) {
          values[i] = docValues.nextValue();
        }
      } else {
        size = 0;
      }
    }

    @Override
    public Long get(int index) {
      return getLong(index);
    }

    public long getLong(int index) {
      if (size == 0) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values[index];
    }

    @Override
    public int size() {
      return size;
    }

    public long getValue() {
      return getLong(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(getLong(index)).build();
    }
  }

  public static final class SortedFloats extends LoadedDocValues<Float> {
    private final SortedNumericDocValues docValues;
    private float[] values = new float[0];
    private int size;

    public SortedFloats(SortedNumericDocValues docValues) {
      this.docValues = docValues;
      this.size = 0;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        size = docValues.docValueCount();
        values = ArrayUtil.grow(values, size);
        for (int i = 0; i < size; ++i) {
          values[i] = NumericUtils.sortableIntToFloat((int) docValues.nextValue());
        }
      } else {
        size = 0;
      }
    }

    @Override
    public Float get(int index) {
      return getFloat(index);
    }

    public float getFloat(int index) {
      if (size == 0) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values[index];
    }

    @Override
    public int size() {
      return size;
    }

    public float getValue() {
      return getFloat(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(getFloat(index)).build();
    }
  }

  public static final class SortedDoubles extends LoadedDocValues<Double> {
    private final SortedNumericDocValues docValues;
    private double[] values = new double[0];
    private int size;

    public SortedDoubles(SortedNumericDocValues docValues) {
      this.docValues = docValues;
      this.size = 0;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        size = docValues.docValueCount();
        values = ArrayUtil.grow(values, size);
        for (int i = 0; i < size; ++i) {
          values[i] = NumericUtils.sortableLongToDouble(docValues.nextValue());
        }
      } else {
        size = 0;
      }
    }

    @Override
    public Double get(int index) {
      return getDouble(index);
    }

    public double getDouble(int index) {
      if (size == 0) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values[index];
    }

    @Override
    public int size() {
      return size;
    }

    public double getValue() {
      return getDouble(0);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(getDouble(index)).build();
    }
  }

  // Even single points use SortedNumericDocValues, since they are LatLonDocValuesFields
  public static final class SingleLocation extends SortedNumericValues<GeoPoint> {
    public SingleLocation(SortedNumericDocValues docValues) {
      super(docValues, GEO_POINT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      GeoPoint point = get(index);
      LatLng latLon =
          LatLng.newBuilder().setLatitude(point.getLat()).setLongitude(point.getLon()).build();
      return SearchResponse.Hit.FieldValue.newBuilder().setLatLngValue(latLon).build();
    }

    public double arcDistance(double lat, double lon) {
      return getValue().arcDistance(lat, lon);
    }
  }

  public static final class Locations extends SortedNumericValues<GeoPoint> {
    public Locations(SortedNumericDocValues docValues) {
      super(docValues, GEO_POINT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      GeoPoint point = get(index);
      LatLng latLon =
          LatLng.newBuilder().setLatitude(point.getLat()).setLongitude(point.getLon()).build();
      return SearchResponse.Hit.FieldValue.newBuilder().setLatLngValue(latLon).build();
    }

    public double arcDistance(double lat, double lon) {
      // backward compatible with ES
      // assume we actually only have 1 location while declared as multivalued
      return get(0).arcDistance(lat, lon);
    }
  }

  public static final class SortedDateTimes extends SortedNumericValues<Instant> {
    public SortedDateTimes(SortedNumericDocValues docValues) {
      super(docValues, DATE_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      long epochMs = get(index).toEpochMilli();
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(epochMs).build();
    }
  }

  public static final class ObjectJsonDocValues extends LoadedDocValues<Struct> {
    private final BinaryDocValues docValues;
    private ArrayList<Struct> value = new ArrayList<>();

    public ObjectJsonDocValues(BinaryDocValues docValues) {
      this.docValues = docValues;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      value.clear();
      if (docValues.advanceExact(docID)) {
        String jsonString = STRING_DECODER.apply(docValues.binaryValue());
        ListValue.Builder builder = ListValue.newBuilder();
        JsonFormat.parser().merge(jsonString, builder);
        List<Value> valueList = builder.getValuesList();
        value.ensureCapacity(valueList.size());
        for (int i = 0; i < valueList.size(); ++i) {
          value.add(valueList.get(i).getStructValue());
        }
      }
    }

    @Override
    public Struct get(int index) {
      if (value.isEmpty()) {
        throw new IllegalStateException("No doc values for document");
      }
      return value.get(index);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      Struct struct = get(index);
      return SearchResponse.Hit.FieldValue.newBuilder().setStructValue(struct).build();
    }

    @Override
    public int size() {
      return value.size();
    }

    public Struct getValue() {
      return get(0);
    }
  }

  public abstract static class SingleBinaryBase<T> extends LoadedDocValues<T> {
    private final BinaryDocValues docValues;
    private final Function<BytesRef, T> decoder;
    private T value;

    public SingleBinaryBase(BinaryDocValues docValues, Function<BytesRef, T> decoder) {
      this.docValues = docValues;
      this.decoder = decoder;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = decoder.apply(docValues.binaryValue());
      } else {
        value = null;
      }
    }

    @Override
    public T get(int index) {
      if (value == null) {
        throw new IllegalStateException("No doc values for document");
      } else if (index != 0) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return value;
    }

    @Override
    public int size() {
      return value == null ? 0 : 1;
    }

    public T getValue() {
      return get(0);
    }
  }

  public static final class SingleBinary extends SingleBinaryBase<BytesRef> {
    public SingleBinary(BinaryDocValues docValues) {
      super(docValues, BYTES_REF_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder()
          .setTextValue(get(index).utf8ToString())
          .build();
    }
  }

  public static final class SingleString extends SingleBinaryBase<String> {
    public SingleString(BinaryDocValues docValues) {
      super(docValues, STRING_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setTextValue(get(index)).build();
    }
  }

  public static final class SortedStrings extends LoadedDocValues<String> {
    private final SortedSetDocValues docValues;
    private final ArrayList<String> values = new ArrayList<>();

    public SortedStrings(SortedSetDocValues docValues) {
      this.docValues = docValues;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      values.clear();
      if (docValues.advanceExact(docID)) {
        long ord = docValues.nextOrd();
        while (ord != SortedSetDocValues.NO_MORE_ORDS) {
          values.add(docValues.lookupOrd(ord).utf8ToString());
          ord = docValues.nextOrd();
        }
      }
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setTextValue(get(index)).build();
    }

    @Override
    public String get(int index) {
      if (values.isEmpty()) {
        throw new IllegalStateException("No doc values for document");
      }
      return values.get(index);
    }

    @Override
    public int size() {
      return values.size();
    }

    public String getValue() {
      return get(0);
    }
  }
}
