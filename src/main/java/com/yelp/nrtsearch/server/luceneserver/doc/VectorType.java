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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/** Placeholder for vectorType and associated operations */
public final class VectorType implements List<Float> {

  /** float[] array used to hold the vector data */
  protected float[] vectorData;

  /** The number of elements in the vector */
  protected int vectorSize;

  /** Constructor to create a float array with a default size of 10 */
  public VectorType() {
    this(10);
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

  /**
   * Construct an empty VectorType with the given capacity
   *
   * @param vectorCapacity @Throws IllegalArgumentException – if the specified capacity is negative
   */
  public VectorType(int vectorCapacity) {
    if (vectorCapacity < 0) {
      throw new IllegalArgumentException("vector array capacity cannot be less than 0");
    }
    this.vectorData = new float[vectorCapacity];
    this.vectorSize = vectorCapacity;
  }

  /** @return the number of elements in the vectorType */
  @Override
  public int size() {
    return vectorSize;
  }

  /** @return current allocated capacity of vectorType */
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
      checkVectorCapacity(vectorSize + 1);
      vectorData[vectorSize++] = vectorElement;
    }
    return true;
  }

  public void checkVectorCapacity(int requiredCapacity) {
    if (vectorData.length >= requiredCapacity) {
      return;
    }
    float[] newArray = new float[requiredCapacity];
    System.arraycopy(vectorData, 0, newArray, 0, vectorSize);
    vectorData = newArray;
  }

  public float[] getVectorData() {
    return vectorData;
  }

  @Override
  public boolean remove(Object elem) {
    return false;
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

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends Float> c) {
    return false;
  }

  @Override
  public boolean addAll(int index, Collection<? extends Float> c) {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {}

  @Override
  public Float get(int index) {
    return null;
  }

  @Override
  public Float set(int index, Float element) {
    return null;
  }

  @Override
  public void add(int index, Float element) {}

  @Override
  public Float remove(int index) {
    return null;
  }

  @Override
  public int indexOf(Object o) {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<Float> listIterator() {
    return null;
  }

  @Override
  public ListIterator<Float> listIterator(int index) {
    return null;
  }

  @Override
  public List<Float> subList(int fromIndex, int toIndex) {
    return null;
  }
}
