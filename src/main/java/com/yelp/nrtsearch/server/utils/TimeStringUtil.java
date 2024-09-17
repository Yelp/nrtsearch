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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Utility class for generating and validating time strings. */
public class TimeStringUtil {

  private static final DateTimeFormatter MSEC_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
  private static final DateTimeFormatter SEC_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

  private TimeStringUtil() {}

  /** Generate a unique time string based on the current UTC time formatted as yyyyMMddHHmmssSSS. */
  public static String generateTimeStringMs() {
    return MSEC_FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
  }

  /** Generate a time string based on the current UTC time formatted as yyyyMMddHHmmss. */
  public static String generateTimeStringSec() {
    return SEC_FORMATTER.format(LocalDateTime.now(ZoneId.of("UTC")));
  }

  /** Check if the given string is a valid time string. */
  public static boolean isTimeStringMs(String timeString) {
    try {
      MSEC_FORMATTER.parse(timeString);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /** Check if the given string is a valid time string. */
  public static boolean isTimeStringSec(String timeString) {
    try {
      SEC_FORMATTER.parse(timeString);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Format the given time as a UTC time string formatted as yyyyMMddHHmmssSSS.
   *
   * @param time time to format
   * @return formatted time string
   */
  public static String formatTimeStringMs(Instant time) {
    return MSEC_FORMATTER.format(time.atZone(ZoneId.of("UTC")));
  }

  /**
   * Format the given time as a UTC time string formatted as yyyyMMddHHmmss.
   *
   * @param time time to format
   * @return formatted time string
   */
  public static String formatTimeStringSec(Instant time) {
    return SEC_FORMATTER.format(time.atZone(ZoneId.of("UTC")));
  }

  /**
   * Parse the given UTC time string of the form yyyyMMddHHmmssSSS as an Instant.
   *
   * @param timeString time string to parse
   * @return parsed Instant
   */
  public static Instant parseTimeStringMs(String timeString) {
    return LocalDateTime.parse(timeString, MSEC_FORMATTER).atZone(ZoneId.of("UTC")).toInstant();
  }

  /**
   * Parse the given UTC time string of the form yyyyMMddHHmmss as an Instant.
   *
   * @param timeString time string to parse
   * @return parsed Instant
   */
  public static Instant parseTimeStringSec(String timeString) {
    return LocalDateTime.parse(timeString, SEC_FORMATTER).atZone(ZoneId.of("UTC")).toInstant();
  }
}
