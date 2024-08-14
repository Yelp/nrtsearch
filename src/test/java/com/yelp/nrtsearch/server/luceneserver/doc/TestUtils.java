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

import static org.junit.Assert.*;

public class TestUtils {

  public static void assertNoDocValues(Runnable r) {
    try {
      r.run();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No doc values for document", e.getMessage());
    }
  }

  public static void assertOutOfBounds(Runnable r) {
    try {
      r.run();
      fail();
    } catch (IndexOutOfBoundsException e) {
      assertTrue(e.getMessage().startsWith("No doc value for index: "));
    }
  }
}
