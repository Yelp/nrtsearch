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
package com.yelp.nrtsearch.server.field;

import com.yelp.nrtsearch.server.grpc.Field;
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

  /**
   * Create a new instance of the current {@link FieldDef} that is updated to use the provided
   * request field definition. This method must not modify the current instance, but instead return
   * a new instance with the updated properties.
   *
   * @param name name of the field
   * @param requestField the field definition from the request
   * @param context context for creating the field definition
   * @return a new instance of the field definition with updated properties
   */
  public FieldDef createUpdatedFieldDef(
      String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
    throw new UnsupportedOperationException(
        String.format("Field %s does not support update", this.getName()));
  }

  /** Get String representation of the field type. */
  public abstract String getType();

  /**
   * Get the facet value type for this field.
   *
   * @return field facet value type
   */
  public abstract IndexableFieldDef.FacetValueType getFacetValueType();

  /**
   * Get if the global ordinals for this field should be created up front with each new {@link
   * org.apache.lucene.index.IndexReader}. Only supported for SORTED_SET_DOC_VALUES facet type.
   */
  public boolean getEagerGlobalOrdinals() {
    return false;
  }

  @Override
  public void close() {}
}
