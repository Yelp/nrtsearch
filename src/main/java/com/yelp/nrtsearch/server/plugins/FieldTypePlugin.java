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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefProvider;
import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for providing custom field types. These types can be used by registering a field
 * with the CUSTOM {@link com.yelp.nrtsearch.server.grpc.FieldType} and specifying the type name in
 * the {@link com.yelp.nrtsearch.server.grpc.Field} additionalProperties {@link
 * com.google.protobuf.Struct}.
 *
 * <pre>
 *     ...
 *     additionalProperties: {
 *         "type": "custom_type_name"
 *     }
 *     ...
 * </pre>
 */
public interface FieldTypePlugin {

  /**
   * Provide a map of custom field types to register. These field types will be usable by name with
   * the CUSTOM {@link com.yelp.nrtsearch.server.grpc.FieldType}.
   *
   * @return mapping from new field type names to {@link FieldDefProvider}
   */
  default Map<String, FieldDefProvider<? extends FieldDef>> getFieldTypes() {
    return Collections.emptyMap();
  }
}
