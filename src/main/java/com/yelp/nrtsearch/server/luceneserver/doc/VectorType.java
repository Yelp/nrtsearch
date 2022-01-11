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

import com.google.common.primitives.Floats;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/** Placeholder for vectorType and associated operations */
public final class VectorType implements List<Float> {

  /** Default vector capacity */
  private static final int DEFAULT_CAPACITY = 10;

  /** float[] array used to hold the vector data */
  private float[] vectorData;

  /** The number of elements in the vector */
  private int vectorSize;

  /**
   * Amount by which to inflate the vector capacity when size exceeds capacity. If capacityIncrement
   * <= 0, the vector size is doubled
   */
  private int capacityIncrement;

  /** Constructor to create a float array with default size */
  public VectorType() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Construct VectorType with given vector data
   *
   * @param vectorData
   */
  public VectorType(float[] vectorData) {
    this.vectorData = vectorData;
    this.vectorSize = vectorData.length;
  }

  public VectorType(int initialCapacity, int capacityIncrement) {
    super();
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("vector capacity should be >= 0");
    }
    this.vectorData = new float[initialCapacity];
    this.capacityIncrement = capacityIncrement;
  }

  /**
   * Constructor to create a float array with given initial capacity
   *
   * @param initialCapacity initial vector capacity @Throws IllegalArgumentException – if the
   *     specified capacity is negative
   */
  public VectorType(int initialCapacity) {
    this(initialCapacity, 0);
  }

  /** @return number of elements in the vector */
  @Override
  public int size() {
    return vectorSize;
  }

  /** @return current allocated capacity of the vector */
  public int getCapacity() {
    return vectorData.length;
  }

  @Override
  public boolean isEmpty() {
    return vectorSize == 0;
  }

  @Override
  public boolean contains(Object obj) {
    return obj instanceof Float && (Floats.contains(vectorData, ((Float) obj).floatValue()));
  }

  @Override
  public boolean add(Float vectorElement) {
    if (vectorSize == vectorData.length) {
      ensureCapacity(vectorSize + 1);
      vectorData[vectorSize++] = vectorElement;
    }
    return true;
  }

  public void ensureCapacity(int requiredCapacity) {
    if (requiredCapacity > 0 && requiredCapacity > vectorData.length) {
      growVector(requiredCapacity);
    }
  }

  private float[] growVector(int capacity) {
    /**
     * float[] newArray = new float[requiredCapacity]; System.arraycopy(vectorData, 0, newArray, 0,
     * vectorSize); vectorData = newArray;
     */
    int oldCapacity = vectorData.length;
    int newCapacity = (capacityIncrement <= 0) ? oldCapacity * 2 : oldCapacity + capacityIncrement;
    return vectorData = Arrays.copyOf(vectorData, Math.max(newCapacity, capacity));
  }

  public float[] getVectorData() {
    return vectorData;
  }

  @Override
  public boolean remove(Object elem) {
    return removeElement(elem);
  }

  public boolean removeElement(Object obj) {
    int idx = indexOf(obj, 0);
    if (idx >= 0) {
      remove(idx);
      return true;
    }
    return false;
  }

  public int indexOf(Object obj, int index) {
    if (obj instanceof Float) {
      for (int i = index; i < vectorSize; i++) {
        if (equals(((Float) obj).floatValue(), vectorData[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  private boolean equals(float floatValue1, float floatValue2) {
    return Float.compare(floatValue1, floatValue2) == 0;
  }

  public void removeElementAt(int index) {
    remove(index);
  }

  // TODO: Needs to be implemented
  @Override
  public Iterator<Float> iterator() {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  // TODO: Needs to be implemented
  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  // TODO: Needs to be implemented
  @Override
  public boolean addAll(Collection<? extends Float> c) {
    return false;
  }

  // TODO: Needs to be implemented
  @Override
  public boolean addAll(int index, Collection<? extends Float> c) {
    return false;
  }

  // TODO: Needs to be implemented
  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  // TODO: Needs to be implemented
  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  // TODO: Needs to be implemented
  @Override
  public void clear() {}

  // TODO: Needs to be implemented
  @Override
  public Float get(int index) {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public Float set(int index, Float element) {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public void add(int index, Float element) {}

  // TODO: Needs to be implemented
  @Override
  public Float remove(int index) {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public int indexOf(Object o) {
    return 0;
  }

  // TODO: Needs to be implemented
  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  // TODO: Needs to be implemented
  @Override
  public ListIterator<Float> listIterator() {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public ListIterator<Float> listIterator(int index) {
    return null;
  }

  // TODO: Needs to be implemented
  @Override
  public List<Float> subList(int fromIndex, int toIndex) {
    return null;
  }
}
