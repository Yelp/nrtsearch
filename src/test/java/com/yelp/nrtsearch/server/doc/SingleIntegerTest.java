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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.junit.Test;

public class SingleIntegerTest {

  private void verifyUnset(LoadedDocValues.SingleInteger loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.getInt(0));
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SingleInteger loadedData, int value) {
    assertEquals(1, loadedData.size());
    assertEquals(value, loadedData.get(0).intValue());
    assertEquals(value, loadedData.getValue());
    assertEquals(value, loadedData.getInt(0));
    assertEquals(
        SearchResponse.Hit.FieldValue.newBuilder().setIntValue(value).build(),
        loadedData.toFieldValue(0));
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SingleInteger loadedData = new LoadedDocValues.SingleInteger(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.longValue())
        .thenReturn((long) Integer.MAX_VALUE, 15L, (long) Integer.MIN_VALUE);

    LoadedDocValues.SingleInteger loadedData = new LoadedDocValues.SingleInteger(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Integer.MAX_VALUE);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 15);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, Integer.MIN_VALUE);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.longValue()).thenReturn(15L);

    LoadedDocValues.SingleInteger loadedData = new LoadedDocValues.SingleInteger(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.getInt(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(1));
    TestUtils.assertOutOfBounds(() -> loadedData.getInt(1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(1));
  }
}
