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

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import org.apache.lucene.index.SortedNumericDocValues;
import org.junit.Test;

public class SortedBooleansTest {

  private void verifyUnset(LoadedDocValues.SortedBooleans loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.getBoolean(0));
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SortedBooleans loadedData, boolean... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i));
      assertEquals(values[i], loadedData.getBoolean(i));
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setBooleanValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SortedBooleans loadedData = new LoadedDocValues.SortedBooleans(null);
    assertEquals(0, loadedData.size());
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.docValueCount()).thenReturn(1, 2, 1);
    when(mockDocValues.nextValue()).thenReturn(1L, 0L, 1L, 0L);

    LoadedDocValues.SortedBooleans loadedData = new LoadedDocValues.SortedBooleans(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, true);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, false, true);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, false);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.docValueCount()).thenReturn(2);
    when(mockDocValues.nextValue()).thenReturn(1L, 0L);

    LoadedDocValues.SortedBooleans loadedData = new LoadedDocValues.SortedBooleans(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, true, false);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.getBoolean(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(2));
    TestUtils.assertOutOfBounds(() -> loadedData.getBoolean(2));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
