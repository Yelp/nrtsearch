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

import java.io.Closeable;

/** Base class for all field definition types. */
public abstract class FieldDef implements Closeable {
  private final String name;

  /**
   * Protected constructor to handle setting the field name.
   *
   * @param name name of field
   */
  protected FieldDef(String name) {
    this.name = name;
  }

  /** Get the name of this field. */
  public String getName() {
    return name;
  }

  /** Get String representation of the field type. */
  public abstract String getType();

  /**
   * Get the facet value type for this field.
   *
   * @return field facet value type
   */
  public abstract IndexableFieldDef.FacetValueType getFacetValueType();

  @Override
  public void close() {}
}
