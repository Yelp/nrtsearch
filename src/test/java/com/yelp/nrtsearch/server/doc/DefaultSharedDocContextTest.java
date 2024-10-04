/*
 * Copyright 2021 Yelp Inc.
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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.Map;
import org.junit.Test;

public class DefaultSharedDocContextTest {

  @Test
  public void testGetContext() {
    DefaultSharedDocContext context = new DefaultSharedDocContext();
    Map<String, Object> map1 = context.getContext(1);
    Map<String, Object> map2 = context.getContext(1);
    Map<String, Object> map3 = context.getContext(2);
    Map<String, Object> map4 = context.getContext(1);
    Map<String, Object> map5 = context.getContext(2);

    assertSame(map1, map2);
    assertSame(map2, map4);
    assertSame(map3, map5);
    assertNotSame(map1, map3);
  }
}
