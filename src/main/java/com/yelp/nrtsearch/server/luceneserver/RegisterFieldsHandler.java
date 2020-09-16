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
package com.yelp.nrtsearch.server.luceneserver;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefResponse;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefBindings;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.field.IdFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptParamsTransformer;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import com.yelp.nrtsearch.server.utils.StructValueTransformer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.DoubleValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterFieldsHandler implements Handler<FieldDefRequest, FieldDefResponse> {
  static final String CUSTOM_TYPE_KEY = "type";
  Logger logger = LoggerFactory.getLogger(RegisterFieldsHandler.class);
  private final JsonParser jsonParser = new JsonParser();

  /* Sets the FieldDef for every field specified in FieldDefRequest and saves it in IndexState.fields member
   * returns the String representation of the same */
  @Override
  public FieldDefResponse handle(final IndexState indexState, FieldDefRequest fieldDefRequest)
      throws RegisterFieldsException {
    assert indexState != null;

    final Map<String, FieldDef> pendingFieldDefs = new HashMap<>();
    final Map<String, String> saveStates = new HashMap<>();
    Set<String> seen = new HashSet<>();

    // We make two passes.  In the first pass, we do the
    // "real" fields, and second pass does the virtual
    // fields, so that any fields the virtual field
    // references are guaranteed to exist, in a single
    // request (or, from the saved json):
    for (int pass = 0; pass < 2; pass++) {
      List<Field> fields = fieldDefRequest.getFieldList();
      for (Field currentField : fields) {
        String fieldName = currentField.getName();

        if (pass == 1 && seen.contains(fieldName)) {
          continue;
        }

        //                if (!(currentField.getValue() instanceof JSONObject)) {
        //                    r.fail("field \"" + fieldName + "\": expected object containing the
        // field type but got: " + ent.getValue());
        //                }

        ;
        if (pass == 0 && FieldType.VIRTUAL.equals(currentField.getType())) {
          // Do this on 2nd pass so the field it refers to will be registered even if it's a single
          // request
          continue;
        }

        if (!IndexState.isSimpleName(fieldName)) {
          throw new RegisterFieldsException(
              "invalid field name \"" + fieldName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
        }

        if (fieldName.endsWith("_boost")) {
          throw new RegisterFieldsException(
              "invalid field name \"" + fieldName + "\": field names cannot end with _boost");
        }

        if (seen.contains(fieldName)) {
          throw new RegisterFieldsException(
              "field \"" + fieldName + "\" appears at least twice in this request");
        }

        seen.add(fieldName);

        try {
          // convert Proto object to Json String
          String currentFieldAsJsonString = JsonFormat.printer().print(currentField);
          saveStates.put(fieldName, currentFieldAsJsonString);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }

        FieldDef fieldDef =
            parseOneFieldType(indexState, pendingFieldDefs, fieldName, currentField);
        if (fieldDef instanceof IdFieldDef) {
          verifyOnlyOneIdFieldExists(indexState, pendingFieldDefs, fieldDef);
        }
        pendingFieldDefs.put(fieldName, fieldDef);
      }
    }

    // add FieldDef and its corresponding JsonObject to states variable in IndexState
    for (Map.Entry<String, FieldDef> ent : pendingFieldDefs.entrySet()) {
      JsonObject fieldAsJsonObject =
          jsonParser.parse(saveStates.get(ent.getKey())).getAsJsonObject();
      indexState.addField(ent.getValue(), fieldAsJsonObject);
    }
    String response = indexState.getAllFieldsJSON();
    FieldDefResponse reply = FieldDefResponse.newBuilder().setResponse(response).build();
    return reply;
  }

  private void verifyOnlyOneIdFieldExists(
      IndexState indexState, Map<String, FieldDef> pendingFieldDefs, FieldDef fieldDef)
      throws RegisterFieldsException {
    IdFieldDef existingIdField = indexState.getIdFieldDef();
    if (existingIdField == null) {
      for (Map.Entry<String, FieldDef> f : pendingFieldDefs.entrySet()) {
        if (f.getValue() instanceof IdFieldDef) {
          existingIdField = (IdFieldDef) f.getValue();
          break;
        }
      }
    }
    if (existingIdField != null) {
      throw new RegisterFieldsException(
          String.format(
              "cannot register another _id field \"%s\" as an _id field \"%s\" already exists",
              fieldDef.getName(), existingIdField.getName()));
    }
  }

  private FieldDef parseOneFieldType(
      IndexState indexState,
      Map<String, FieldDef> pendingFieldDefs,
      String fieldName,
      Field currentField)
      throws RegisterFieldsException {
    FieldType fieldType = currentField.getType();
    if (FieldType.VIRTUAL.equals(currentField.getType())) {
      return parseOneVirtualFieldType(indexState, pendingFieldDefs, fieldName, currentField);
    }

    String fieldTypeStr;
    if (fieldType.equals(FieldType.CUSTOM)) {
      fieldTypeStr =
          getCustomFieldType(
              StructValueTransformer.transformStruct(currentField.getAdditionalProperties()));
    } else {
      fieldTypeStr = fieldType.name();
    }
    FieldDef fieldDef =
        FieldDefCreator.getInstance().createFieldDef(fieldName, fieldTypeStr, currentField);
    // handle fields with facets. We may want to rethink where this happens once facets
    // are fully functional.
    if (fieldDef instanceof IndexableFieldDef) {
      IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
      IndexableFieldDef.FacetValueType facetType = indexableFieldDef.getFacetValueType();
      if (facetType != IndexableFieldDef.FacetValueType.NO_FACETS
          && facetType != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
        // hierarchy, float or sortedSetDocValues
        if (facetType == IndexableFieldDef.FacetValueType.HIERARCHY) {
          indexState.facetsConfig.setHierarchical(fieldName, true);
        }
        if (indexableFieldDef.isMultiValue()) {
          indexState.facetsConfig.setMultiValued(fieldName, true);
        }
        // set indexFieldName for HIERARCHY (TAXO), SORTED_SET_DOC_VALUE and FLAT  facet
        String facetFieldName =
            currentField.getFacetIndexFieldName().isEmpty()
                ? String.format("$_%s", currentField.getName())
                : currentField.getFacetIndexFieldName();
        indexState.facetsConfig.setIndexFieldName(fieldName, facetFieldName);
      }
    }
    // nocommit facetsConfig.setRequireDimCount
    logger.info("REGISTER: " + fieldName + " -> " + fieldDef);
    return fieldDef;
  }

  /** Messy: we need this for indexed-but-not-tokenized fields, solely for .getOffsetGap I think. */
  public static final Analyzer dummyAnalyzer =
      new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          throw new UnsupportedOperationException();
        }
      };

  private FieldDef parseOneVirtualFieldType(
      IndexState indexState,
      Map<String, FieldDef> pendingFieldDefs,
      String fieldName,
      Field currentField)
      throws RegisterFieldsException {
    ScoreScript.Factory factory =
        ScriptService.getInstance().compile(currentField.getScript(), ScoreScript.CONTEXT);
    Map<String, Object> params =
        Maps.transformValues(
            currentField.getScript().getParamsMap(), ScriptParamsTransformer.INSTANCE);
    // Workaround for the fact the the javascript expression may need bindings to other fields in
    // this request.
    // Build the complete bindings and pass it as a script parameter. We might want to think about a
    // better way of
    // doing this (or maybe updating index state in general).
    if (currentField.getScript().getLang().equals(JsScriptEngine.LANG)) {
      params = new HashMap<>(params);

      Map<String, FieldDef> allFields = new HashMap<>(indexState.getAllFields());
      allFields.putAll(pendingFieldDefs);
      params.put("bindings", new FieldDefBindings(allFields));
    }
    DoubleValuesSource values = factory.newFactory(params, indexState.docLookup);

    return new VirtualFieldDef(fieldName, values);
  }

  public static class RegisterFieldsException extends Handler.HandlerException {
    public RegisterFieldsException(String errorMessage) {
      super(errorMessage);
    }

    public RegisterFieldsException(String errorMessage, Throwable err) {
      super(errorMessage, err);
    }
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
}
