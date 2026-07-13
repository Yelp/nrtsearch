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
package com.yelp.nrtsearch.server.concurrent;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class LoggingUncaughtExceptionHandlerTest {

  @Test
  public void testGetInstanceReturnsSingleton() {
    assertSame(
        LoggingUncaughtExceptionHandler.getInstance(),
        LoggingUncaughtExceptionHandler.getInstance());
  }

  @Test
  public void testGetInstanceNotNull() {
    assertNotNull(LoggingUncaughtExceptionHandler.getInstance());
  }

  @Test
  public void testUncaughtExceptionDoesNotThrow() {
    Thread thread = new Thread(() -> {}, "test-thread");
    // Should not throw even for severe errors like OOME
    LoggingUncaughtExceptionHandler.getInstance()
        .uncaughtException(thread, new RuntimeException("test"));
    LoggingUncaughtExceptionHandler.getInstance()
        .uncaughtException(thread, new OutOfMemoryError("simulated OOM"));
  }
}
