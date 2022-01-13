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

import com.google.gson.Gson;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue.Vector;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue.Vector.Builder;
import com.yelp.nrtsearch.server.luceneserver.geo.GeoPoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
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
  // long decoders
  private static final LongFunction<Boolean> BOOL_DECODER = (longValue) -> longValue == 1;
  private static final LongFunction<Integer> INT_DECODER = (longValue) -> (int) longValue;
  private static final LongFunction<Long> LONG_DECODER = (longValue) -> longValue;
  private static final LongFunction<Float> FLOAT_DECODER =
      (longValue) -> Float.intBitsToFloat((int) longValue);
  private static final LongFunction<Float> SORTED_FLOAT_DECODER =
      (longValue) -> NumericUtils.sortableIntToFloat((int) longValue);
  private static final LongFunction<Double> DOUBLE_DECODER = Double::longBitsToDouble;
  private static final LongFunction<Double> SORTED_DOUBLE_DECODER =
      NumericUtils::sortableLongToDouble;
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

  // Gson decoder to deserilize string to objects
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

  public static final class SingleBoolean extends SingleNumericValue<Boolean> {
    public SingleBoolean(NumericDocValues docValues) {
      super(docValues, BOOL_DECODER);
    }

    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue(get(index)).build();
    }
  }

  public static final class SingleInteger extends SingleNumericValue<Integer> {
    public SingleInteger(NumericDocValues docValues) {
      super(docValues, INT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setIntValue(get(index)).build();
    }
  }

  public static final class SingleLong extends SingleNumericValue<Long> {
    public SingleLong(NumericDocValues docValues) {
      super(docValues, LONG_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(get(index)).build();
    }
  }

  public static final class SingleFloat extends SingleNumericValue<Float> {
    public SingleFloat(NumericDocValues docValues) {
      super(docValues, FLOAT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(get(index)).build();
    }
  }

  public static final class SingleDouble extends SingleNumericValue<Double> {
    public SingleDouble(NumericDocValues docValues) {
      super(docValues, DOUBLE_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(get(index)).build();
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
      values.trimToSize();
    }

    @Override
    public T get(int index) {
      if (values.isEmpty()) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= values.size()) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values.get(index);
    }

    @Override
    public int size() {
      return values.size();
    }
  }

  public static final class SortedBooleans extends SortedNumericValues<Boolean> {
    public SortedBooleans(SortedNumericDocValues docValues) {
      super(docValues, BOOL_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue(get(index)).build();
    }
  }

  public static final class SortedIntegers extends SortedNumericValues<Integer> {
    public SortedIntegers(SortedNumericDocValues docValues) {
      super(docValues, INT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setIntValue(get(index)).build();
    }
  }

  public static final class SortedLongs extends SortedNumericValues<Long> {
    public SortedLongs(SortedNumericDocValues docValues) {
      super(docValues, LONG_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setLongValue(get(index)).build();
    }
  }

  public static final class SortedFloats extends SortedNumericValues<Float> {
    public SortedFloats(SortedNumericDocValues docValues) {
      super(docValues, SORTED_FLOAT_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(get(index)).build();
    }
  }

  public static final class SortedDoubles extends SortedNumericValues<Double> {
    public SortedDoubles(SortedNumericDocValues docValues) {
      super(docValues, SORTED_DOUBLE_DECODER);
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(get(index)).build();
    }
  }

  // Even single points use SortedNumericDocValues, since they are LatLonDocValuesFields
  public static final class SingleLocation extends SortedNumericValues<GeoPoint> {
    public SingleLocation(SortedNumericDocValues docValues) {
      super(docValues, GEO_POINT_DECODER);
    }

    public GeoPoint getValue() {
      return get(0);
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
    private List<Struct> value;

    public ObjectJsonDocValues(BinaryDocValues docValues) {
      this.docValues = docValues;
    }

    @Override
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        String jsonString = STRING_DECODER.apply(docValues.binaryValue());
        ListValue.Builder builder = ListValue.newBuilder();
        JsonFormat.parser().merge(jsonString, builder);
        value =
            builder.getValuesList().stream()
                .map(e -> e.getStructValue())
                .collect(Collectors.toList());
      } else {
        value = null;
      }
    }

    @Override
    public Struct get(int index) {
      if (value == null) {
        throw new IllegalStateException("No doc values for document");
      }
      try {
        return value.get(index);
      } catch (IndexOutOfBoundsException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      Struct struct = get(index);
      return SearchResponse.Hit.FieldValue.newBuilder().setStructValue(struct).build();
    }

    @Override
    public int size() {
      return value == null ? 0 : value.size();
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
      values.trimToSize();
    }

    @Override
    public SearchResponse.Hit.FieldValue toFieldValue(int index) {
      return SearchResponse.Hit.FieldValue.newBuilder().setTextValue(get(index)).build();
    }

    @Override
    public String get(int index) {
      if (values.isEmpty()) {
        throw new IllegalStateException("No doc values for document");
      } else if (index < 0 || index >= values.size()) {
        throw new IndexOutOfBoundsException("No doc value for index: " + index);
      }
      return values.get(index);
    }

    @Override
    public int size() {
      return values.size();
    }
  }

  public static final class SingleVector extends LoadedDocValues<VectorType> {
    private final BinaryDocValues docValues;
    private VectorType value;

    public SingleVector(BinaryDocValues docValues) {
      this.docValues = docValues;
    }

    /**
     * Set method to set the lucene level doc id to lookup value from index and initialize the
     * loaded doc value index by loading vector data
     */
    public void setDocId(int docID) throws IOException {
      if (docValues.advanceExact(docID)) {
        value = decodeBytesRefToVectorType(docValues.binaryValue());
      } else {
        value = null;
      }
    }

    /** Decodes binary doc value to float array and wraps it into a VectorType */
    private static VectorType decodeBytesRefToVectorType(BytesRef bytesRef) {
      float[] floats = new float[bytesRef.length / Float.BYTES];
      FloatBuffer fb =
          ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).asFloatBuffer();
      fb.get(floats);
      return new VectorType(floats);
    }

    /** Provide field value containing the doc value data for a given index */
    @Override
    public FieldValue toFieldValue(int index) {
      VectorType vector = get(index);
      Builder vectorBuilder = Vector.newBuilder();
      for (float value : vector.getVectorData()) {
        vectorBuilder.addValue(value);
      }
      return SearchResponse.Hit.FieldValue.newBuilder()
          .setVectorValue(vectorBuilder.build())
          .build();
    }

    @Override
    public VectorType get(int index) {
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

    public VectorType getValue() {
      return get(0);
    }
  }
}
