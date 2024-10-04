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
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class SortedDoublesTest {

  private void verifyUnset(LoadedDocValues.SortedDoubles loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.getDouble(0));
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SortedDoubles loadedData, double... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue(), 0.0);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i), 0.0);
      assertEquals(values[i], loadedData.getDouble(i), 0.0);
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SortedDoubles loadedData = new LoadedDocValues.SortedDoubles(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.docValueCount()).thenReturn(6, 10, 3);
    when(mockDocValues.nextValue())
        .thenReturn(
            NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY),
            NumericUtils.doubleToSortableLong(15.0f),
            NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY),
            NumericUtils.doubleToSortableLong(Double.NaN),
            NumericUtils.doubleToSortableLong(Double.MAX_VALUE),
            NumericUtils.doubleToSortableLong(Double.MIN_VALUE),
            NumericUtils.doubleToSortableLong(0),
            NumericUtils.doubleToSortableLong(1),
            NumericUtils.doubleToSortableLong(2),
            NumericUtils.doubleToSortableLong(3),
            NumericUtils.doubleToSortableLong(4),
            NumericUtils.doubleToSortableLong(5),
            NumericUtils.doubleToSortableLong(6),
            NumericUtils.doubleToSortableLong(7),
            NumericUtils.doubleToSortableLong(8),
            NumericUtils.doubleToSortableLong(9),
            NumericUtils.doubleToSortableLong(-1),
            NumericUtils.doubleToSortableLong(-2),
            NumericUtils.doubleToSortableLong(-3));

    LoadedDocValues.SortedDoubles loadedData = new LoadedDocValues.SortedDoubles(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(
        loadedData,
        Double.NEGATIVE_INFINITY,
        15.0,
        Double.POSITIVE_INFINITY,
        Double.NaN,
        Double.MAX_VALUE,
        Double.MIN_VALUE);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, -1, -2, -3);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.docValueCount()).thenReturn(2);
    when(mockDocValues.nextValue())
        .thenReturn(
            NumericUtils.doubleToSortableLong(15.0), NumericUtils.doubleToSortableLong(16.0));

    LoadedDocValues.SortedDoubles loadedData = new LoadedDocValues.SortedDoubles(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15.0, 16.0);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.getDouble(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(2));
    TestUtils.assertOutOfBounds(() -> loadedData.getDouble(2));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
