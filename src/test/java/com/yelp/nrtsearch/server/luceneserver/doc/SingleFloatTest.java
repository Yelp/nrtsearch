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
import org.apache.lucene.index.NumericDocValues;
import org.junit.Test;

public class SingleFloatTest {

  private void verifyUnset(LoadedDocValues.SingleFloat loadedData) {
    assertEquals(0, loadedData.size());
    assertNoDocValues(() -> loadedData.get(0));
    assertNoDocValues(loadedData::getValue);
    assertNoDocValues(() -> loadedData.getFloat(0));
    assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SingleFloat loadedData, float value) {
    assertEquals(1, loadedData.size());
    assertEquals(value, loadedData.get(0), 0.0);
    assertEquals(value, loadedData.getValue(), 0.0);
    assertEquals(value, loadedData.getFloat(0), 0.0);
    assertEquals(
        SearchResponse.Hit.FieldValue.newBuilder().setFloatValue(value).build(),
        loadedData.toFieldValue(0));
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SingleFloat loadedData = new LoadedDocValues.SingleFloat(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt()))
        .thenReturn(true, true, true, true, true, true, false);
    when(mockDocValues.longValue())
        .thenReturn(
            (long) Float.floatToRawIntBits(Float.NEGATIVE_INFINITY),
            (long) Float.floatToRawIntBits(15.0f),
            (long) Float.floatToRawIntBits(Float.POSITIVE_INFINITY),
            (long) Float.floatToRawIntBits(Float.NaN),
            (long) Float.floatToRawIntBits(Float.MAX_VALUE),
            (long) Float.floatToRawIntBits(Float.MIN_VALUE));

    LoadedDocValues.SingleFloat loadedData = new LoadedDocValues.SingleFloat(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Float.NEGATIVE_INFINITY);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 15.0f);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, Float.POSITIVE_INFINITY);

    loadedData.setDocId(3);
    verifySetToValue(loadedData, Float.NaN);

    loadedData.setDocId(4);
    verifySetToValue(loadedData, Float.MAX_VALUE);

    loadedData.setDocId(5);
    verifySetToValue(loadedData, Float.MIN_VALUE);

    loadedData.setDocId(6);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.longValue()).thenReturn((long) Float.floatToRawIntBits(15.0f));

    LoadedDocValues.SingleFloat loadedData = new LoadedDocValues.SingleFloat(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15.0f);

    assertOutOfBounds(() -> loadedData.get(-1));
    assertOutOfBounds(() -> loadedData.getFloat(-1));
    assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    assertOutOfBounds(() -> loadedData.get(1));
    assertOutOfBounds(() -> loadedData.getFloat(1));
    assertOutOfBounds(() -> loadedData.toFieldValue(1));
  }
}
