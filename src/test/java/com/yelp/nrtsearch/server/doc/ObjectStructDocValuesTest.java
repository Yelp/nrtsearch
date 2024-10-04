/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.doc;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class ObjectStructDocValuesTest {
  private void verifyUnset(LoadedDocValues.ObjectStructDocValues loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(
      LoadedDocValues.ObjectStructDocValues loadedData, Struct... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i));
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setStructValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.ObjectStructDocValues loadedData =
        new LoadedDocValues.ObjectStructDocValues(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    BinaryDocValues mockDocValues = mock(BinaryDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);

    Struct struct1 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(15).build())
            .build();
    ListValue value1 =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStructValue(struct1).build())
            .build();

    Struct struct2 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(0).build())
            .putFields("key2", Value.newBuilder().setStringValue("1").build())
            .build();
    Struct struct3 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(-1).build())
            .putFields("key2", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    ListValue value2 =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStructValue(struct2).build())
            .addValues(Value.newBuilder().setStructValue(struct3).build())
            .build();

    Struct struct4 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(1000).build())
            .putFields("key2", Value.newBuilder().setBoolValue(true).build())
            .build();
    ListValue value3 =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStructValue(struct4).build())
            .build();

    when(mockDocValues.binaryValue())
        .thenReturn(
            new BytesRef(value1.toByteArray()),
            new BytesRef(value2.toByteArray()),
            new BytesRef(value3.toByteArray()));

    LoadedDocValues.ObjectStructDocValues loadedData =
        new LoadedDocValues.ObjectStructDocValues(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, struct1);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, struct2, struct3);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, struct4);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    BinaryDocValues mockDocValues = mock(BinaryDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);

    Struct struct1 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(0).build())
            .putFields("key2", Value.newBuilder().setStringValue("1").build())
            .build();
    Struct struct2 =
        Struct.newBuilder()
            .putFields("key1", Value.newBuilder().setNumberValue(-1).build())
            .putFields("key2", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    ListValue value =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStructValue(struct1).build())
            .addValues(Value.newBuilder().setStructValue(struct2).build())
            .build();

    when(mockDocValues.binaryValue()).thenReturn(new BytesRef(value.toByteArray()));

    LoadedDocValues.ObjectStructDocValues loadedData =
        new LoadedDocValues.ObjectStructDocValues(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, struct1, struct2);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(2));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
