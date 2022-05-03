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

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.plugins.FieldTypePlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to handle the creation of {@link FieldDef} instances. Type strings are mapped to {@link
 * FieldDefProvider}s to produce concrete {@link FieldDef}s.
 */
public class FieldDefCreator {
  static final String CUSTOM_TYPE_KEY = "type";
  private static FieldDefCreator instance;

  private final Map<String, FieldDefProvider<? extends FieldDef>> fieldDefMap = new HashMap<>();

  public FieldDefCreator(LuceneServerConfiguration configuration) {
    register("ATOM", AtomFieldDef::new);
    register("TEXT", TextFieldDef::new);
    register("BOOLEAN", BooleanFieldDef::new);
    register("LONG", LongFieldDef::new);
    register("INT", IntFieldDef::new);
    register("DOUBLE", DoubleFieldDef::new);
    register("FLOAT", FloatFieldDef::new);
    register("LAT_LON", LatLonFieldDef::new);
    register("POLYGON", PolygonfieldDef::new);
    register("DATE_TIME", DateTimeFieldDef::new);
    register("OBJECT", ObjectFieldDef::new);
    register("_ID", IdFieldDef::new);
    // It would be nice for this to be the factory for virtual fields too,
    // but javascript expression compilation depends on fields that are not
    // completely registered.
    register(
        "VIRTUAL",
        (name, field) -> {
          throw new UnsupportedOperationException("Virtual fields should be created directly");
        });
    register("VECTOR", VectorFieldDef::new);
    register("CONTEXT_SUGGEST", ContextSuggestFieldDef::new);
  }

  /**
   * Get a new {@link FieldDef} instance for a field with the given name and grpc request {@link
   * Field} definition.
   *
   * @param name field name
   * @param field grpc request field definition
   * @return field definition
   */
  public FieldDef createFieldDef(String name, Field field) {
    String type;
    if (field.getType().equals(FieldType.CUSTOM)) {
      type =
          getCustomFieldType(
              StructValueTransformer.transformStruct(field.getAdditionalProperties()));
    } else {
      type = field.getType().name();
    }

    FieldDefProvider<?> provider = fieldDefMap.get(type);
    if (provider == null) {
      throw new IllegalArgumentException("Invalid field type: " + type);
    }
    return provider.get(name, field);
  }

  private void register(Map<String, FieldDefProvider<? extends FieldDef>> fieldDefs) {
    fieldDefs.forEach(this::register);
  }

  private void register(String name, FieldDefProvider<? extends FieldDef> fieldDef) {
    if (fieldDefMap.containsKey(name)) {
      throw new IllegalArgumentException("FieldDef " + name + " already exists");
    }
    fieldDefMap.put(name, fieldDef);
  }

  private String getCustomFieldType(Map<String, ?> additionalProperties) {
    Object typeObject = additionalProperties.get(CUSTOM_TYPE_KEY);
    if (typeObject == null) {
      throw new IllegalArgumentException(
          "Custom fields must specify additionalProperties: " + CUSTOM_TYPE_KEY);
    }
    if (!(typeObject instanceof String)) {
      throw new IllegalArgumentException("Custom type must be a String, found: " + typeObject);
    }
    return typeObject.toString();
  }

  /**
   * Initialize singleton instance of {@link FieldDefCreator}. Registers all the standard field
   * types and any additional types provided by {@link FieldTypePlugin}s.
   *
   * @param configuration service configuration
   * @param plugins list of loaded plugins
   */
  public static void initialize(LuceneServerConfiguration configuration, Iterable<Plugin> plugins) {
    instance = new FieldDefCreator(configuration);
    for (Plugin plugin : plugins) {
      if (plugin instanceof FieldTypePlugin) {
        FieldTypePlugin fieldTypePlugin = (FieldTypePlugin) plugin;
        instance.register(fieldTypePlugin.getFieldTypes());
      }
    }
  }

  /** Get singleton instance. */
  public static FieldDefCreator getInstance() {
    return instance;
  }
}
