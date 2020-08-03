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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

/** Basic lucene field to index and store field data. */
public class FieldWithData extends Field {
  /**
   * Create a field for indexing. The field type is expected to have any needed indexing/stored
   * options set.
   *
   * @param name name of field
   * @param type lucene field type information
   * @param data data that should be indexed
   */
  public FieldWithData(String name, FieldType type, Object data) {
    super(name, type);
    fieldsData = data;
  }
}
