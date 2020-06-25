/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.config;

import java.util.function.Function;

/**
 * Helper Function to convert a yaml read Object that is expected to be a String or Number. Used as
 * a generic way to read types that extend Number, though this is not a strict requirement.
 *
 * @param <T> type to convert the config object into
 */
public class NumberReaderFunc<T> implements Function<Object, T> {
  private final Function<Number, T> numberConverter;
  private final Function<String, T> stringConverter;
  private final Class<T> clazz;

  NumberReaderFunc(
      Class<T> clazz, Function<Number, T> numberConverter, Function<String, T> stringConverter) {
    this.numberConverter = numberConverter;
    this.stringConverter = stringConverter;
    this.clazz = clazz;
  }

  @Override
  public T apply(Object o) {
    if (o instanceof String) {
      return stringConverter.apply((String) o);
    }
    if (o instanceof Number) {
      Number num = (Number) o;
      if (clazz.isInstance(num)) {
        return clazz.cast(num);
      }
      return numberConverter.apply(num);
    }
    return null;
  }
}
