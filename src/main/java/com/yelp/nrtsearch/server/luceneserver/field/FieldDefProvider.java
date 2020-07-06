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
package com.yelp.nrtsearch.server.luceneserver.field;

import com.yelp.nrtsearch.server.grpc.Field;

/**
 * Interface to produce a {@link FieldDef} instance from the given field name and grpc request field
 * definition.
 *
 * @param <T> provided field def type
 */
@FunctionalInterface
public interface FieldDefProvider<T extends FieldDef> {
  /**
   * Get {@link FieldDef} for a field with the given name and grpc request field definition.
   *
   * @param name field name
   * @param requestField field request definition
   * @return concrete {@link FieldDef} instance for field
   */
  T get(String name, Field requestField);
}
