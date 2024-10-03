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
import java.time.ZoneId;
import org.junit.Test;
import org.mockito.MockedStatic;

public class TimeStringUtilsTest {

  @Test
  public void testGenerateTimeStringMs() {
    LocalDateTime mockTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    try (MockedStatic<LocalDateTime> mockLocalDateTime = mockStatic(LocalDateTime.class)) {
      mockLocalDateTime.when(() -> LocalDateTime.now(ZoneId.of("UTC"))).thenReturn(mockTime);
      String timeString = TimeStringUtils.generateTimeStringMs();
      assertEquals("20240820123456789", timeString);
    }
  }

  @Test
  public void testGenerateTimeStringSec() {
    LocalDateTime mockTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    try (MockedStatic<LocalDateTime> mockLocalDateTime = mockStatic(LocalDateTime.class)) {
      mockLocalDateTime.when(() -> LocalDateTime.now(ZoneId.of("UTC"))).thenReturn(mockTime);
      String timeString = TimeStringUtils.generateTimeStringSec();
      assertEquals("20240820123456", timeString);
    }
  }

  @Test
  public void testIsTimeStringMs() {
    assertTrue(TimeStringUtils.isTimeStringMs("20240820123456789"));
    assertTrue(TimeStringUtils.isTimeStringMs("19701010000000000"));
    assertTrue(TimeStringUtils.isTimeStringMs("20391229233759999"));

    assertFalse(TimeStringUtils.isTimeStringMs("20391229233759"));
    assertFalse(TimeStringUtils.isTimeStringMs("20241329233759999"));
    assertFalse(TimeStringUtils.isTimeStringMs("20241232233759999"));
    assertFalse(TimeStringUtils.isTimeStringMs("20241231243759999"));
    assertFalse(TimeStringUtils.isTimeStringMs("09d9c9e4-483e-4a90-9c4F-D342c8da1210"));
    assertFalse(TimeStringUtils.isTimeStringMs("09d9c9e4-483e-4a90-D342c8da1210"));
    assertFalse(TimeStringUtils.isTimeStringMs("other_file"));
    assertFalse(TimeStringUtils.isTimeStringMs("_3.cfs"));
    assertFalse(TimeStringUtils.isTimeStringMs("segments"));
  }

  @Test
  public void testIsTimeStringSec() {
    assertTrue(TimeStringUtils.isTimeStringSec("20240820123456"));
    assertTrue(TimeStringUtils.isTimeStringSec("19701010000000"));
    assertTrue(TimeStringUtils.isTimeStringSec("20391229233759"));

    assertFalse(TimeStringUtils.isTimeStringSec("20391229233759000"));
    assertFalse(TimeStringUtils.isTimeStringSec("20241329233759"));
    assertFalse(TimeStringUtils.isTimeStringSec("20241232233759"));
    assertFalse(TimeStringUtils.isTimeStringSec("20241231243759"));
    assertFalse(TimeStringUtils.isTimeStringSec("09d9c9e4-483e-4a90-9c4F-D342c8da1210"));
    assertFalse(TimeStringUtils.isTimeStringSec("09d9c9e4-483e-4a90-D342c8da1210"));
    assertFalse(TimeStringUtils.isTimeStringSec("other_file"));
    assertFalse(TimeStringUtils.isTimeStringSec("_3.cfs"));
    assertFalse(TimeStringUtils.isTimeStringSec("segments"));
  }

  @Test
  public void testFormatTimeStringMs() {
    LocalDateTime localDateTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    String timeString =
        TimeStringUtils.formatTimeStringMs(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
    assertEquals("20240820123456789", timeString);
  }

  @Test
  public void testFormatTimeStringSec() {
    LocalDateTime localDateTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    String timeString =
        TimeStringUtils.formatTimeStringSec(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
    assertEquals("20240820123456", timeString);
  }

  @Test
  public void testParseTimeStringMs() {
    LocalDateTime localDateTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56, 789000000);
    String timeString = "20240820123456789";
    assertEquals(
        localDateTime.atZone(ZoneId.of("UTC")).toInstant(),
        TimeStringUtils.parseTimeStringMs(timeString));
  }

  @Test
  public void testParseTimeStringSec() {
    LocalDateTime localDateTime = LocalDateTime.of(2024, 8, 20, 12, 34, 56);
    String timeString = "20240820123456";
    assertEquals(
        localDateTime.atZone(ZoneId.of("UTC")).toInstant(),
        TimeStringUtils.parseTimeStringSec(timeString));
  }
}
