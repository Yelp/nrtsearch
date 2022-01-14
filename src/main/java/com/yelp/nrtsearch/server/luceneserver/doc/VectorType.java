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

import java.lang.reflect.Array;
import java.util.AbstractList;

public final class VectorType extends AbstractList<Float> {

  private float[] vectorData;

  /**
   * Construct VectorType with given vector data
   *
   * @param vectorData
   */
  public VectorType(float[] vectorData) {
    this.vectorData = vectorData;
  }

  /**
   * Constructor to create a float array with given initial capacity
   *
   * @param initialCapacity initial vector capacity @Throws IllegalArgumentException – if the
   *     specified capacity is negative
   */
  public VectorType(int initialCapacity) {
    super();
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("vector capacity should be >= 0");
    }
    this.vectorData = new float[initialCapacity];
  }

  @Override
  public Float get(int index) {
    return Array.getFloat(vectorData, index);
  }

  @Override
  public Float set(int index, Float element) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException(
          "vector index value: <" + index + "> is not within vector capacity");
    }
    float currentValue = get(index);
    vectorData[index] = element;
    return currentValue;
  }

  /** @return number of elements in the vector */
  @Override
  public int size() {
    return vectorData.length;
  }

  public float[] getVectorData() {
    return vectorData;
  }
}
