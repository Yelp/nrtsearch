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
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class SortedFloatsTest {

  private void verifyUnset(LoadedDocValues.SortedFloats loadedData) {
    assertEquals(0, loadedData.size());
    assertNoDocValues(() -> loadedData.get(0));
    assertNoDocValues(loadedData::getValue);
    assertNoDocValues(() -> loadedData.getFloat(0));
    assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SortedFloats loadedData, float... values) {
    assertEquals(values.length, loadedData.size());
    assertEquals(values[0], loadedData.getValue(), 0.0);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], loadedData.get(i), 0.0);
      assertEquals(values[i], loadedData.getFloat(i), 0.0);
      assertEquals(
          SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(values[i]).build(),
          loadedData.toFieldValue(i));
    }
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SortedFloats loadedData = new LoadedDocValues.SortedFloats(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    SortedNumericDocValues mockDocValues = mock(SortedNumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.docValueCount()).thenReturn(6, 10, 3);
    when(mockDocValues.nextValue())
        .thenReturn(
            (long) NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY),
            (long) NumericUtils.floatToSortableInt(15.0f),
            (long) NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY),
            (long) NumericUtils.floatToSortableInt(Float.NaN),
            (long) NumericUtils.floatToSortableInt(Float.MAX_VALUE),
            (long) NumericUtils.floatToSortableInt(Float.MIN_VALUE),
            (long) NumericUtils.floatToSortableInt(0),
            (long) NumericUtils.floatToSortableInt(1),
            (long) NumericUtils.floatToSortableInt(2),
            (long) NumericUtils.floatToSortableInt(3),
            (long) NumericUtils.floatToSortableInt(4),
            (long) NumericUtils.floatToSortableInt(5),
            (long) NumericUtils.floatToSortableInt(6),
            (long) NumericUtils.floatToSortableInt(7),
            (long) NumericUtils.floatToSortableInt(8),
            (long) NumericUtils.floatToSortableInt(9),
            (long) NumericUtils.floatToSortableInt(-1),
            (long) NumericUtils.floatToSortableInt(-2),
            (long) NumericUtils.floatToSortableInt(-3));

    LoadedDocValues.SortedFloats loadedData = new LoadedDocValues.SortedFloats(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(
        loadedData,
        Float.NEGATIVE_INFINITY,
        15.0f,
        Float.POSITIVE_INFINITY,
        Float.NaN,
        Float.MAX_VALUE,
        Float.MIN_VALUE);

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
            (long) NumericUtils.floatToSortableInt(15.0f),
            (long) NumericUtils.floatToSortableInt(16.0f));

    LoadedDocValues.SortedFloats loadedData = new LoadedDocValues.SortedFloats(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15.0f, 16.0f);

    assertOutOfBounds(() -> loadedData.get(-1));
    assertOutOfBounds(() -> loadedData.getFloat(-1));
    assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    assertOutOfBounds(() -> loadedData.get(2));
    assertOutOfBounds(() -> loadedData.getFloat(2));
    assertOutOfBounds(() -> loadedData.toFieldValue(2));
  }
}
