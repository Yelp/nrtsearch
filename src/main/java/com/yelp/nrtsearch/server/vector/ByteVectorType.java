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
package com.yelp.nrtsearch.server.vector;

import java.lang.reflect.Array;
import java.util.AbstractList;

public final class ByteVectorType extends AbstractList<Byte> {

  private final byte[] vectorData;

  /**
   * Construct ByteVectorType with given vector data
   *
   * @param vectorData
   */
  public ByteVectorType(byte[] vectorData) {
    this.vectorData = vectorData;
  }

  /**
   * Constructor to create a byte array with given initial capacity
   *
   * @param initialCapacity initial vector capacity @Throws IllegalArgumentException – if the
   *     specified capacity is negative
   */
  public ByteVectorType(int initialCapacity) {
    super();
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("vector capacity should be >= 0");
    }
    this.vectorData = new byte[initialCapacity];
  }

  @Override
  public Byte get(int index) {
    return Array.getByte(vectorData, index);
  }

  @Override
  public Byte set(int index, Byte element) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException(
          "vector index value: <" + index + "> is not within vector capacity");
    }
    byte currentValue = get(index);
    vectorData[index] = element;
    return currentValue;
  }

  /**
   * @return number of elements in the vector
   */
  @Override
  public int size() {
    return vectorData.length;
  }

  public byte[] getVectorData() {
    return vectorData;
  }
}
