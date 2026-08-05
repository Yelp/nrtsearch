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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Test;

public class LoggingThreadFactoryTest {

  @Test
  public void testCreatedThreadHasLoggingHandler() {
    LoggingThreadFactory factory = new LoggingThreadFactory(new NamedThreadFactory("test"));
    Thread thread = factory.newThread(() -> {});
    assertSame(LoggingUncaughtExceptionHandler.getInstance(), thread.getUncaughtExceptionHandler());
  }

  @Test
  public void testCreatedThreadHasNameFromDelegate() {
    LoggingThreadFactory factory = new LoggingThreadFactory(new NamedThreadFactory("mypool"));
    Thread thread = factory.newThread(() -> {});
    assertTrue(thread.getName().startsWith("mypool-"));
  }
}
