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

import com.google.protobuf.Descriptors;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.search.DoubleValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static helper class to handle a request to add/update fields. */
public class FieldUpdateUtils {
  private static final Logger logger = LoggerFactory.getLogger(FieldUpdateUtils.class);
  // The name is not actually changeable, but it is allowed to be present to identify the field to
  // update
  private static final Set<String> ALLOWED_UPDATABLE_FIELDS = Set.of("name", "childFields");

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
    FieldAndFacetState.Builder fieldStateBuilder =
        initializeFieldStateBuilder(currentState, currentFields, updateFields);
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
      Field updatedField = getUpdatedField(field, newFields.get(field.getName()));
      parseField(
          updatedField, currentState.getFields().get(field.getName()), fieldStateBuilder, context);
      newFields.put(updatedField.getName(), updatedField);
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
   * @param previousFieldDef current field definition, or null if this is a new field
   * @param fieldStateBuilder builder for new field state
   * @param context creation context
   */
  public static void parseField(
      Field field,
      FieldDef previousFieldDef,
      FieldAndFacetState.Builder fieldStateBuilder,
      FieldDefCreator.FieldDefCreatorContext context) {
    FieldDef fieldDef;
    if (previousFieldDef != null) {
      fieldDef =
          FieldDefCreator.getInstance()
              .createFieldDefFromPrevious(field.getName(), field, previousFieldDef, context);
    } else {
      fieldDef = FieldDefCreator.getInstance().createFieldDef(field.getName(), field, context);
    }
    fieldStateBuilder.addField(fieldDef, field);
    if (fieldDef instanceof IndexableFieldDef) {
      addChildFields((IndexableFieldDef<?>) fieldDef, fieldStateBuilder);
    }
    logger.info("REGISTER: " + fieldDef.getName() + " -> " + fieldDef);
  }

  /**
   * Initialize a {@link FieldAndFacetState.Builder} with the current fields, excluding any fields
   * that are being updated.
   *
   * @param currentState current state of the fields
   * @param currentFields current fields
   * @param updateFields fields to update
   * @return a new {@link FieldAndFacetState.Builder} initialized with non-updated fields
   */
  private static FieldAndFacetState.Builder initializeFieldStateBuilder(
      FieldAndFacetState currentState,
      Map<String, Field> currentFields,
      Iterable<Field> updateFields) {
    Set<String> updateFieldNames =
        StreamSupport.stream(updateFields.spliterator(), false)
            .map(Field::getName)
            .collect(Collectors.toSet());

    FieldAndFacetState.Builder fieldStateBuilder = new FieldAndFacetState().toBuilder();
    for (Map.Entry<String, Field> entry : currentFields.entrySet()) {
      String fieldName = entry.getKey();
      Field field = entry.getValue();
      if (!updateFieldNames.contains(fieldName)) {
        FieldDef fieldDef = currentState.getFields().get(fieldName);
        fieldStateBuilder.addField(fieldDef, field);
        if (fieldDef instanceof IndexableFieldDef) {
          addChildFields((IndexableFieldDef<?>) fieldDef, fieldStateBuilder);
        }
      }
    }
    return fieldStateBuilder;
  }

  /**
   * Get the updated {@link Field} based on the new field and the existing field. If the existing
   * field is null, it returns the new field. If the new field has only updatable properties, it
   * updates the existing field with the new properties and returns it.
   *
   * @param newField the new field or update
   * @param oldField the existing field, or null if this is a new field
   * @return fully materialized {@link Field} with updated properties
   */
  private static Field getUpdatedField(Field newField, Field oldField) {
    Objects.requireNonNull(newField);
    if (oldField == null) {
      return newField;
    }

    if (hasOnlyUpdatableProperties(newField)) {
      Map<String, Field> updatedChildFields = new HashMap<>();
      for (Field childField : oldField.getChildFieldsList()) {
        updatedChildFields.put(childField.getName(), childField);
      }
      List<Field> newChildFields = new ArrayList<>();

      for (Field childField : newField.getChildFieldsList()) {
        Field updatedChildField =
            getUpdatedField(childField, updatedChildFields.get(childField.getName()));
        if (updatedChildFields.containsKey(updatedChildField.getName())) {
          updatedChildFields.put(updatedChildField.getName(), updatedChildField);
        } else {
          newChildFields.add(updatedChildField);
        }
      }

      // Add old fields first to maintain order in rebuilt Field
      List<Field> finalChildFields = new ArrayList<>();
      for (Field childField : oldField.getChildFieldsList()) {
        finalChildFields.add(updatedChildFields.get(childField.getName()));
      }
      finalChildFields.addAll(newChildFields);

      Field.Builder fieldBuilder = oldField.toBuilder();
      fieldBuilder.clearChildFields();
      fieldBuilder.addAllChildFields(finalChildFields);

      return fieldBuilder.build();
    } else {
      throw new IllegalArgumentException("Duplicate field registration: " + newField.getName());
    }
  }

  /**
   * Check if the field has only updatable properties.
   *
   * @param field the field to check
   * @return true if the field has only updatable properties, false otherwise
   */
  static boolean hasOnlyUpdatableProperties(Field field) {
    if (field.getChildFieldsCount() == 0) {
      return false;
    }
    Set<String> fieldKeys =
        field.getAllFields().keySet().stream()
            .map(Descriptors.FieldDescriptor::getName)
            .collect(Collectors.toSet());
    fieldKeys.removeAll(ALLOWED_UPDATABLE_FIELDS);
    if (fieldKeys.isEmpty()) {
      return true;
    } else {
      logger.warn(
          "Unable to update field {}: properties {} are not updatable", field.getName(), fieldKeys);
      return false;
    }
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
