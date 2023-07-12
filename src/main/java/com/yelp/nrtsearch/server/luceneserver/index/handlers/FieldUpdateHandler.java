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
package com.yelp.nrtsearch.server.luceneserver.index.handlers;

import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefResponse;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.index.FieldAndFacetState;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.DoubleValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static helper class to handle a request to add/update fields. */
public class FieldUpdateHandler {
  private static final Logger logger = LoggerFactory.getLogger(FieldUpdateHandler.class);

  private FieldUpdateHandler() {}

  /**
   * Handle a FieldDefRequest.
   *
   * @param indexStateManager state manager for index
   * @param request request message
   * @return response message
   * @throws IOException on error committing state
   */
  public static FieldDefResponse handle(
      IndexStateManager indexStateManager, FieldDefRequest request) throws IOException {
    String updatedFields = indexStateManager.updateFields(request.getFieldList());
    return FieldDefResponse.newBuilder().setResponse(updatedFields).build();
  }

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
   * @return state after applying field updates
   */
  public static UpdatedFieldInfo updateFields(
      FieldAndFacetState currentState,
      Map<String, Field> currentFields,
      Iterable<Field> updateFields) {

    Map<String, Field> newFields = new HashMap<>(currentFields);
    FieldAndFacetState.Builder fieldStateBuilder = currentState.toBuilder();
    List<Field> nonVirtualFields = new ArrayList<>();
    List<Field> childNonVirtualFields = new ArrayList<>();
    List<Field> virtualFields = new ArrayList<>();

    for (Field field : updateFields) {
      if (FieldType.VIRTUAL.equals(field.getType())) {
        checkFieldName(field.getName());
        virtualFields.add(field);
      } else {
        int lastSeparator = field.getName().lastIndexOf(IndexState.CHILD_FIELD_SEPARATOR);
        if (lastSeparator >= 0) {
          checkFieldName(field.getName().substring(lastSeparator + 1));
          childNonVirtualFields.add(field);
        } else {
          checkFieldName(field.getName());
          nonVirtualFields.add(field);
        }
      }
    }

    for (Field field : nonVirtualFields) {
      if (newFields.containsKey(field.getName())) {
        throw new IllegalArgumentException("Duplicate field registration: " + field.getName());
      }
      parseField(field, fieldStateBuilder);
      newFields.put(field.getName(), field);
    }

    for (Field field : childNonVirtualFields) {
      validateAndAddChildField(newFields, field);
      parseField(field, fieldStateBuilder);
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

  public static void validateAndAddChildField(Map<String, Field> fieldMap, Field childField) {
    String[] pathTokens = childField.getName().split("\\.");
    if (!fieldMap.containsKey(pathTokens[0])) {
      throw new IllegalArgumentException(
          "Root field " + pathTokens[0] + " doesn't exist for field " + childField.getName());
    }
    Field.Builder currentField = fieldMap.get(pathTokens[0]).toBuilder();
    for (int i = 1; i < pathTokens.length - 1; i++) {
      String token = pathTokens[i];
      currentField =
          currentField.getChildFieldsBuilderList().stream()
              .filter(f -> token.equals(f.getName()))
              .findAny()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Parent field "
                              + token
                              + " doesn't exist for field "
                              + childField.getName()));
    }
    String fieldBaseName = pathTokens[pathTokens.length - 1];
    if (currentField.getChildFieldsList().stream()
        .filter(f -> fieldBaseName.equals(f.getName()))
        .findAny()
        .isPresent()) {
      throw new IllegalArgumentException(
          "Duplicate field registration: "
              + childField.getName()
              + " under its parent field: "
              + String.join(
                  IndexState.CHILD_FIELD_SEPARATOR,
                  Arrays.copyOf(pathTokens, pathTokens.length - 1)));
    }
    fieldMap.put(
        pathTokens[0],
        currentField.addChildFields(childField.toBuilder().setName(fieldBaseName)).build());
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
   */
  public static void parseField(Field field, FieldAndFacetState.Builder fieldStateBuilder) {
    FieldDef fieldDef = FieldDefCreator.getInstance().createFieldDef(field.getName(), field);
    fieldStateBuilder.addField(fieldDef, field);
    if (fieldDef instanceof IndexableFieldDef) {
      addChildFields((IndexableFieldDef) fieldDef, fieldStateBuilder);

      // When adding a child field, we need to update its parent field's child map
      int lastSeparator = field.getName().lastIndexOf(IndexState.CHILD_FIELD_SEPARATOR);
      if (lastSeparator >= 0) {
        String parentFieldName = field.getName().substring(0, lastSeparator);
        FieldDef parentField = fieldStateBuilder.getField(parentFieldName);
        if (parentField instanceof IndexableFieldDef) {
          ((IndexableFieldDef) parentField)
              .getChildFields()
              .put(fieldDef.getName(), (IndexableFieldDef) fieldDef);
        }
      }
    }
    logger.info("REGISTER: " + fieldDef.getName() + " -> " + fieldDef);
  }

  // recursively add all children to pendingFieldDefs
  private static void addChildFields(
      IndexableFieldDef indexableFieldDef, FieldAndFacetState.Builder fieldStateBuilder) {
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
    // Workaround for the fact that the javascript expression may need bindings to other fields in
    // this request.
    // Build the complete bindings and pass it as a script parameter. We might want to think about a
    // better way of
    // doing this (or maybe updating index state in general).
    if (field.getScript().getLang().equals(JsScriptEngine.LANG)) {
      params = new HashMap<>(params);
      params.put("bindings", fieldStateBuilder.getBindings());
    } else {
      // TODO fix this, by removing DocLookup dependency on IndexState. Should be possible to just
      // use the fields from the field state builder
      throw new IllegalArgumentException("Only js lang supported for index virtual fields");
    }
    // js scripts use Bindings instead of DocLookup
    DoubleValuesSource values = factory.newFactory(params, null);

    FieldDef virtualFieldDef = new VirtualFieldDef(field.getName(), values);
    fieldStateBuilder.addField(virtualFieldDef, field);
    logger.info("REGISTER: " + virtualFieldDef.getName() + " -> " + virtualFieldDef);
  }
}
