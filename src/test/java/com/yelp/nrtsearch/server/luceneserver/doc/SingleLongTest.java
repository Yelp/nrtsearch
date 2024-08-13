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

public class SingleLongTest {

  private void verifyUnset(LoadedDocValues.SingleLong loadedData) {
    assertEquals(0, loadedData.size());
    assertNoDocValues(() -> loadedData.get(0));
    assertNoDocValues(loadedData::getValue);
    assertNoDocValues(() -> loadedData.getLong(0));
    assertNoDocValues(() -> loadedData.toFieldValue(0));
  }

  private void verifySetToValue(LoadedDocValues.SingleLong loadedData, long value) {
    assertEquals(1, loadedData.size());
    assertEquals(value, loadedData.get(0).longValue());
    assertEquals(value, loadedData.getValue());
    assertEquals(value, loadedData.getLong(0));
    assertEquals(
        SearchResponse.Hit.FieldValue.newBuilder().setLongValue(value).build(),
        loadedData.toFieldValue(0));
  }

  @Test
  public void testNotSet() {
    LoadedDocValues.SingleLong loadedData = new LoadedDocValues.SingleLong(null);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetValue() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true, true, true, false);
    when(mockDocValues.longValue()).thenReturn(Long.MAX_VALUE, 15L, Long.MIN_VALUE);

    LoadedDocValues.SingleLong loadedData = new LoadedDocValues.SingleLong(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, Long.MAX_VALUE);

    loadedData.setDocId(1);
    verifySetToValue(loadedData, 15L);

    loadedData.setDocId(2);
    verifySetToValue(loadedData, Long.MIN_VALUE);

    loadedData.setDocId(3);
    verifyUnset(loadedData);
  }

  @Test
  public void testSetDocValuesOutOfBounds() throws IOException {
    NumericDocValues mockDocValues = mock(NumericDocValues.class);
    when(mockDocValues.advanceExact(anyInt())).thenReturn(true);
    when(mockDocValues.longValue()).thenReturn(15L);

    LoadedDocValues.SingleLong loadedData = new LoadedDocValues.SingleLong(mockDocValues);
    loadedData.setDocId(0);
    verifySetToValue(loadedData, 15L);

    assertOutOfBounds(() -> loadedData.get(-1));
    assertOutOfBounds(() -> loadedData.getLong(-1));
    assertOutOfBounds(() -> loadedData.toFieldValue(-1));

    assertOutOfBounds(() -> loadedData.get(1));
    assertOutOfBounds(() -> loadedData.getLong(1));
    assertOutOfBounds(() -> loadedData.toFieldValue(1));
  }
}
