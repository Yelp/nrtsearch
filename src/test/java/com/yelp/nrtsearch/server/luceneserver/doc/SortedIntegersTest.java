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
package com.yelp.nrtsearch.server.luceneserver.doc;

import static com.yelp.nrtsearch.server.luceneserver.doc.TestUtils.assertNoDocValues;
import static com.yelp.nrtsearch.server.luceneserver.doc.TestUtils.assertOutOfBounds;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import java.io.IOException;
import org.apache.lucene.index.SortedNumericDocValues;
import org.junit.Test;

public class SortedIntegersTest {

  private void verifyUnset(LoadedDocValues.SortedIntegers loadedData) {
    assertEquals(0, loadedData.size());
    assertNoDocValues(() -> loadedData.get(0));
    assertNoDocValues(loadedData::getValue);
    assertNoDocValues(() -> loadedData.getInt(0));
    assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SortedIntegers loadedData, int... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i).intValue());
      assertEquals(values[i], loadedData.getInt(i));
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setIntValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SortedIntegers loadedData = new LoadedDocValues.SortedIntegers(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.docValueCount()).thenReturn(3, 10, 2);
    when(mockDocValues.nextValue())
        .thenReturn(
            (long) Integer.MAX_VALUE,
            15L,
            (long) Integer.MIN_VALUE,
            0L,
            1L,
            2L,
            3L,
            4L,
            5L,
            6L,
            7L,
            8L,
            9L,
            -1L,
            -2L);

    LoadedDocValues.SortedIntegers loadedData = new LoadedDocValues.SortedIntegers(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Integer.MAX_VALUE, 15, Integer.MIN_VALUE);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, -1, -2);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.docValueCount()).thenReturn(2);
    when(mockDocValues.nextValue()).thenReturn((long) 15, (long) 16);

    LoadedDocValues.SortedIntegers loadedData = new LoadedDocValues.SortedIntegers(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15, 16);

    assertOutOfBounds(() -> loadedData.get(-1));
    assertOutOfBounds(() -> loadedData.getInt(-1));
    assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    assertOutOfBounds(() -> loadedData.get(2));
    assertOutOfBounds(() -> loadedData.getInt(2));
    assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
