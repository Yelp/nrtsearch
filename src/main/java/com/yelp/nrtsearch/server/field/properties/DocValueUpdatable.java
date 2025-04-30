/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.field.properties;

import java.util.List;

/**
 * Interface for {@link com.yelp.nrtsearch.server.field.FieldDef} types that can have their doc
 * values updated. For now, Lucene supports updating doc values for numeric and binary fields.
 */
public interface DocValueUpdatable {

  /**
   * determine if this {@link com.yelp.nrtsearch.server.field.FieldDef} can be updated, this will
   * depend on other factors such as, if the field is searchable or is multivalued etc.
   *
   * @return if the field can be updated.
   */
  boolean isUpdatable();

  /**
   * @param value value to be updated
   * @return get the docValue for this {@link com.yelp.nrtsearch.server.field.FieldDef} to update.
   */
  org.apache.lucene.document.Field getUpdatableDocValueField(List<String> value);
}
