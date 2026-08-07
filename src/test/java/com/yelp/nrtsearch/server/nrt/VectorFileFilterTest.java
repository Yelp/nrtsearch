/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.junit.Test;

public class VectorFileFilterTest {

  @Test
  public void testIsRawVectorFile_vecExtension() {
    assertTrue(VectorFileFilter.isRawVectorFile("_0_Lucene99HnswVectorsFormat_0.vec"));
  }

  @Test
  public void testIsRawVectorFile_vemfExtension() {
    assertTrue(VectorFileFilter.isRawVectorFile("_0_Lucene99HnswVectorsFormat_0.vemf"));
  }

  @Test
  public void testIsRawVectorFile_quantizedDataNotRaw() {
    assertFalse(VectorFileFilter.isRawVectorFile("_0_Lucene104ScalarQuantized_0.veq"));
  }

  @Test
  public void testIsRawVectorFile_quantizedMetaNotRaw() {
    assertFalse(VectorFileFilter.isRawVectorFile("_0_Lucene104ScalarQuantized_0.vemq"));
  }

  @Test
  public void testIsRawVectorFile_hnswGraphNotRaw() {
    assertFalse(VectorFileFilter.isRawVectorFile("_0_Lucene99HnswVectorsFormat_0.vem"));
    assertFalse(VectorFileFilter.isRawVectorFile("_0_Lucene99HnswVectorsFormat_0.vex"));
  }

  @Test
  public void testIsRawVectorFile_otherIndexFiles() {
    assertFalse(VectorFileFilter.isRawVectorFile("_0.si"));
    assertFalse(VectorFileFilter.isRawVectorFile("_0.fnm"));
    assertFalse(VectorFileFilter.isRawVectorFile("_0.dvd"));
    assertFalse(VectorFileFilter.isRawVectorFile("segments_3"));
  }

  @Test
  public void testFilterRawVectorFiles_removesVecAndVemf() {
    Map<String, String> files =
        Map.of(
            "_0.vec", "raw-data",
            "_0.vemf", "raw-meta",
            "_0.veq", "quantized-data",
            "_0.vemq", "quantized-meta",
            "_0.vem", "hnsw-meta",
            "_0.vex", "hnsw-index",
            "_0.si", "segment-info");

    Map<String, String> filtered = VectorFileFilter.filterRawVectorFiles(files);

    assertEquals(5, filtered.size());
    assertFalse(filtered.containsKey("_0.vec"));
    assertFalse(filtered.containsKey("_0.vemf"));
    assertTrue(filtered.containsKey("_0.veq"));
    assertTrue(filtered.containsKey("_0.vemq"));
    assertTrue(filtered.containsKey("_0.vem"));
    assertTrue(filtered.containsKey("_0.vex"));
    assertTrue(filtered.containsKey("_0.si"));
  }

  @Test
  public void testFilterRawVectorFiles_emptyMap() {
    Map<String, String> filtered = VectorFileFilter.filterRawVectorFiles(Map.of());
    assertTrue(filtered.isEmpty());
  }

  @Test
  public void testFilterRawVectorFiles_noRawFiles() {
    Map<String, String> files = Map.of("_0.veq", "quantized-data", "_0.si", "segment-info");
    Map<String, String> filtered = VectorFileFilter.filterRawVectorFiles(files);
    assertEquals(2, filtered.size());
  }
}
