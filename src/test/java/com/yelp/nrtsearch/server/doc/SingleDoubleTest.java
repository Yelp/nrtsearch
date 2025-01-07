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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class SingleDoubleTest {

  private void verifyUnset(LoadedDocValues.SingleDouble loadedData) {
    assertEquals(0, loadedData.size());
    TestUtils.assertNoDocValues(() -> loadedData.get(0));
    TestUtils.assertNoDocValues(loadedData::getValue);
    TestUtils.assertNoDocValues(() -> loadedData.getDouble(0));
    TestUtils.assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SingleDouble loadedData, double value) {
    assertEquals(1, loadedData.size());
    assertEquals(value, loadedData.get(0), 0.0);
    assertEquals(value, loadedData.getValue(), 0.0);
    assertEquals(value, loadedData.getDouble(0), 0.0);
    assertEquals(
        SearchResponse.Hit.FieldValue.newBuilder().setDoubleValue(value).build(),
        loadedData.toFieldValue(0));
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SingleDouble loadedData = new LoadedDocValues.SingleDouble(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt()))
        .thenReturn(true, true, true, true, true, true, false);
    when(mockDocValues.longValue())
        .thenReturn(
            NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY),
            NumericUtils.doubleToSortableLong(15.0),
            NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY),
            NumericUtils.doubleToSortableLong(Double.NaN),
            NumericUtils.doubleToSortableLong(Double.MAX_VALUE),
            NumericUtils.doubleToSortableLong(Double.MIN_VALUE));

    LoadedDocValues.SingleDouble loadedData = new LoadedDocValues.SingleDouble(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Double.NEGATIVE_INFINITY);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 15.0);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, Double.POSITIVE_INFINITY);

    loadedData.setDocId(3);
    verifySetToValue(loadedData, Double.NaN);

    loadedData.setDocId(4);
    verifySetToValue(loadedData, Double.MAX_VALUE);

    loadedData.setDocId(5);
    verifySetToValue(loadedData, Double.MIN_VALUE);

    loadedData.setDocId(6);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.longValue()).thenReturn(NumericUtils.doubleToSortableLong(15.0));

    LoadedDocValues.SingleDouble loadedData = new LoadedDocValues.SingleDouble(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15.0);

    TestUtils.assertOutOfBounds(() -> loadedData.get(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.getDouble(-1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    TestUtils.assertOutOfBounds(() -> loadedData.get(1));
    TestUtils.assertOutOfBounds(() -> loadedData.getDouble(1));
    TestUtils.assertOutOfBounds(() -> loadedData.toFieldValue(1));
  }
}
