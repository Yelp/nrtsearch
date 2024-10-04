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

public class SortedLongsTest {

  private void verifyUnset(LoadedDocValues.SortedLongs loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.getLong(0));
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SortedLongs loadedData, long... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue());
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i).longValue());
      assertEquals(values[i], loadedData.getLong(i));
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setLongValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SortedLongs loadedData = new LoadedDocValues.SortedLongs(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.docValueCount()).thenReturn(3, 10, 2);
    when(mockDocValues.nextValue())
        .thenReturn(
            Long.MAX_VALUE, 15L, Long.MIN_VALUE, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, -1L, -2L);

    LoadedDocValues.SortedLongs loadedData = new LoadedDocValues.SortedLongs(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Long.MAX_VALUE, 15L, Long.MIN_VALUE);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, -1L, -2L);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.docValueCount()).thenReturn(2);
    when(mockDocValues.nextValue()).thenReturn(15L, 16L);

    LoadedDocValues.SortedLongs loadedData = new LoadedDocValues.SortedLongs(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15L, 16L);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.getLong(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(2));
    TestUtils.assertOutOfBounds(() -> loadedData.getLong(2));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
