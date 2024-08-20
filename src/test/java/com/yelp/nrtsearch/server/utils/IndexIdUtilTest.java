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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mockStatic;

import java.time.LocalDateTime;
import org.junit.Test;
import org.mockito.MockedStatic;

public class IndexIdUtilTest {

  @Test
  public void testGenerateIndexId() {
    LocalDateTime mockTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    try (MockedStatic<LocalDateTime> mockLocalDateTime = mockStatic(LocalDateTime.class)) {
      mockLocalDateTime.when(LocalDateTime::now).thenReturn(mockTime);
      String indexId = IndexIdUtil.generateIndexId();
      assertEquals("20240820123456789", indexId);
    }
  }

  @Test
  public void testIsIndexId() {
    assertTrue(IndexIdUtil.isIndexId("20240820123456789"));
    assertTrue(IndexIdUtil.isIndexId("19701010000000000"));
    assertTrue(IndexIdUtil.isIndexId("20391229233759999"));

    assertFalse(IndexIdUtil.isIndexId("20241329233759999"));
    assertFalse(IndexIdUtil.isIndexId("20241232233759999"));
    assertFalse(IndexIdUtil.isIndexId("20241231243759999"));
    assertFalse(IndexIdUtil.isIndexId("09d9c9e4-483e-4a90-9c4F-D342c8da1210"));
    assertFalse(IndexIdUtil.isIndexId("09d9c9e4-483e-4a90-D342c8da1210"));
    assertFalse(IndexIdUtil.isIndexId("other_file"));
    assertFalse(IndexIdUtil.isIndexId("_3.cfs"));
    assertFalse(IndexIdUtil.isIndexId("segments"));
  }
}
