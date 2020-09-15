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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types that can
 * be used for keying Documents.
 *
 * <p>There are two purposes for Keyable fields: 1. Only one document can exists with the same key
 * value 2. An AddDocument request with an existing key value will update the document
 */
public interface Keyable {

  /**
   * Get the name of the keyable field This will be used to generate the Term for updating the
   * document
   *
   * @return name of the keyable field
   */
  String getName();

  /**
   * Construct a Term with the given field and value to identify the document to be added or updated
   *
   * @param document the document to be added or updated
   * @return a Term with field and value
   */
  default Term getTerm(Document document) {
    String fieldName = this.getName();
    if (fieldName == null) {
      throw new IllegalArgumentException(
          "the keyable field should have a name to be able to build a Term for updating the document");
    }
    String fieldValue = document.get(fieldName);
    if (fieldValue == null) {
      throw new IllegalArgumentException(
          "document cannot have a null field value for a keyable field");
    }
    return new Term(fieldName, fieldValue);
  }
}
