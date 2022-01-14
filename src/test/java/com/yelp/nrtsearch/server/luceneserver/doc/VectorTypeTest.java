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

import org.junit.Assert;
import org.junit.Test;

public class VectorTypeTest {

  private static final int INITIAL_CAPACITY = 5;
  private static final int DEFAULT_CAPACITY = 10;
  private static final float[] TEST_ARRAY = {1.0f, 2.5f, 1000.1000f};

  /** Test VectorType constructors and size() methods */
  @Test
  public void vectorTypeConstructorTest() {
    VectorType vector = new VectorType(TEST_ARRAY);
    assertEquals(TEST_ARRAY.length, vector.size());

    VectorType vectorWithCapacity = new VectorType(INITIAL_CAPACITY);
    assertEquals(INITIAL_CAPACITY, vectorWithCapacity.size());
  }

  @Test
  public void getTest() {
    VectorType vector = new VectorType(TEST_ARRAY);
    assertEquals(2.5f, vector.get(1), Math.ulp(2.5f));
  }

  @Test
  public void setTest() {
    VectorType vectorWithCapacity = new VectorType(INITIAL_CAPACITY);
    int index = 2;
    float newValue = 2.5f;
    float oldValue = vectorWithCapacity.set(index, newValue);
    assertTrue(0 == oldValue);
    assertTrue(vectorWithCapacity.get(index) == newValue);
  }

  @Test
  public void getVectorDataTest() {
    VectorType vector = new VectorType(TEST_ARRAY);
    assertEquals(TEST_ARRAY, vector.getVectorData());
  }

  @Test
  public void vectorTypeExceptionTest() {
    Exception capacityException =
        Assert.assertThrows(IllegalArgumentException.class, () -> new VectorType(-2));
    assertTrue(capacityException.getMessage().contains("vector capacity should be >= 0"));

    Exception illegalIndexException =
        Assert.assertThrows(
            IndexOutOfBoundsException.class,
            () -> {
              VectorType vector = new VectorType(INITIAL_CAPACITY);
              vector.set(10, 1.0f);
            });
    assertTrue(
        illegalIndexException
            .getMessage()
            .contains("vector index value: <10> is not within vector capacity"));
  }
}
