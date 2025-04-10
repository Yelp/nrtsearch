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
package com.yelp.nrtsearch.server.index;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptService;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.DoubleValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static helper class to handle a request to add/update fields. */
public class FieldUpdateUtils {
  private static final Logger logger = LoggerFactory.getLogger(FieldUpdateUtils.class);

  private FieldUpdateUtils() {}

  /**
   * Container class to hold the {@link Field} definitions used in state, and the corresponding
   * built {@link FieldAndFacetState}.
   */
  public static class UpdatedFieldInfo {
    public final Map<String, Field> fields;
    public final FieldAndFacetState fieldAndFacetState;

    public UpdatedFieldInfo(Map<String, Field> fields, FieldAndFacetState fieldAndFacetState) {
      this.fields = fields;
      this.fieldAndFacetState = fieldAndFacetState;
    }
  }

  /**
   * Get the new {@link UpdatedFieldInfo} from applying a given set of field updates to the current
   * fields. Currently, only supports the addition of new fields.
   *
   * @param currentState built state from the current fields
   * @param currentFields current fields
   * @param updateFields field updates
   * @param context creation context
   * @return state after applying field updates
   */
  public static UpdatedFieldInfo updateFields(
      FieldAndFacetState currentState,
      Map<String, Field> currentFields,
      Iterable<Field> updateFields,
      FieldDefCreator.FieldDefCreatorContext context) {

    Map<String, Field> newFields = new HashMap<>(currentFields);
    FieldAndFacetState.Builder fieldStateBuilder = currentState.toBuilder();
    List<Field> nonVirtualFields = new ArrayList<>();
    List<Field> virtualFields = new ArrayList<>();

    for (Field field : updateFields) {
      checkFieldName(field.getName());
      if (FieldType.VIRTUAL.equals(field.getType())) {
        virtualFields.add(field);
      } else {
        nonVirtualFields.add(field);
      }
    }

    for (Field field : nonVirtualFields) {
      if (newFields.containsKey(field.getName())) {
        throw new IllegalArgumentException("Duplicate field registration: " + field.getName());
      }
      parseField(field, fieldStateBuilder, context);
      newFields.put(field.getName(), field);
    }

    // Process the virtual fields after non-virtual fields, since they may depend on other
    // fields being registered. This is not a complete solution, since virtual fields can
    // reference each other. TODO: fix to handle the chaining virtual field use case.
    for (Field field : virtualFields) {
      if (newFields.containsKey(field.getName())) {
        throw new IllegalArgumentException("Duplicate field registration: " + field.getName());
      }
      parseVirtualField(field, fieldStateBuilder);
      newFields.put(field.getName(), field);
    }

    return new UpdatedFieldInfo(newFields, fieldStateBuilder.build());
  }

  /**
   * Check if the field name is valid.
   *
   * @throws IllegalArgumentException if name is not valid
   */
  public static void checkFieldName(String fieldName) {
    if (!IndexState.isSimpleName(fieldName)) {
      throw new IllegalArgumentException(
          "invalid field name \"" + fieldName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
    }

    if (fieldName.endsWith("_boost")) {
      throw new IllegalArgumentException(
          "invalid field name \"" + fieldName + "\": field names cannot end with _boost");
    }
  }

  /**
   * Parse a non-virtual {@link Field} message and apply changes to the {@link
   * FieldAndFacetState.Builder}.
   *
   * @param field field to process
   * @param fieldStateBuilder builder for new field state
   * @param context creation context
   */
  public static void parseField(
      Field field,
      FieldAndFacetState.Builder fieldStateBuilder,
      FieldDefCreator.FieldDefCreatorContext context) {
    FieldDef fieldDef =
        FieldDefCreator.getInstance().createFieldDef(field.getName(), field, context);
    fieldStateBuilder.addField(fieldDef, field);
    if (fieldDef instanceof IndexableFieldDef) {
      addChildFields((IndexableFieldDef<?>) fieldDef, fieldStateBuilder);
    }
    logger.info("REGISTER: " + fieldDef.getName() + " -> " + fieldDef);
  }

  // recursively add all children to pendingFieldDefs
  private static void addChildFields(
      IndexableFieldDef<?> indexableFieldDef, FieldAndFacetState.Builder fieldStateBuilder) {
    indexableFieldDef
        .getChildFields()
        .forEach(
            (k, v) -> {
              fieldStateBuilder.addField(v, null);
              addChildFields(v, fieldStateBuilder);
            });
  }

  /**
   * Parse a virtual {@link Field} message and apply changes to the {@link
   * FieldAndFacetState.Builder}.
   *
   * @param field virtual field specification
   * @param fieldStateBuilder builder for new field state
   */
  public static void parseVirtualField(Field field, FieldAndFacetState.Builder fieldStateBuilder) {
    ScoreScript.Factory factory =
        ScriptService.getInstance().compile(field.getScript(), ScoreScript.CONTEXT);
    Map<String, Object> params = ScriptParamsUtils.decodeParams(field.getScript().getParamsMap());
    DoubleValuesSource values =
        factory.newFactory(params, new DocLookup(fieldStateBuilder.getFields()::get));

    FieldDef virtualFieldDef = new VirtualFieldDef(field.getName(), values);
    fieldStateBuilder.addField(virtualFieldDef, field);
    logger.info("REGISTER: " + virtualFieldDef.getName() + " -> " + virtualFieldDef);
  }
}
