/*
 * Copyright 2022 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VectorTypeTest {

  @Test
  public void basicVectorTest() {
    float[] floatArr = {1.0f, 2.5f, 1000.1000f};
    VectorType vector = new VectorType(floatArr);
    assertEquals(floatArr.length, vector.size());
  }

  @Test
  public void addTest() {
    float[] floatArr = {1.0f, 2.5f, 1000.1000f};
    VectorType vector = new VectorType(floatArr);
    boolean addResponse = vector.add(2.0f);
    assertEquals(4, vector.size());
    assertTrue(addResponse);
  }

  @Test
  public void getVectorDataTest() {
    float[] floatArr = {1.0f, 2.5f, 1000.1000f};
    VectorType vector = new VectorType(floatArr);
    assertEquals(floatArr, vector.getVectorData());
  }
}
