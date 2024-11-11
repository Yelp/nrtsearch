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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.BooleanFieldDef;
import com.yelp.nrtsearch.server.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.field.FloatFieldDef;
import com.yelp.nrtsearch.server.field.IdFieldDef;
import com.yelp.nrtsearch.server.field.LongFieldDef;
import com.yelp.nrtsearch.server.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.field.TextFieldDef;
import com.yelp.nrtsearch.server.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.grpc.FacetType;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefResponse;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.index.FieldUpdateUtils.UpdatedFieldInfo;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.script.ScriptService;
import com.yelp.nrtsearch.server.script.js.JsScriptEngine;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.assertj.core.util.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

public class FieldUpdateUtilsTest {

  private static final List<Field> simpleUpdates = new ArrayList<>();

  static {
    simpleUpdates.add(
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.FLOAT)
            .setSearch(true)
            .setStoreDocValues(true)
            .build());
    simpleUpdates.add(
        Field.newBuilder()
            .setName("field2")
            .setType(FieldType.LONG)
            .setSearch(false)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());
  }

  @BeforeClass
  public static void setup() {
    String configFile = "nodeName: \"server_foo\"";
    NrtsearchConfig dummyConfig =
        new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes()));
    List<Plugin> dummyPlugins = Collections.emptyList();
    // these must be initialized to create an IndexState
    FieldDefCreator.initialize(dummyConfig, dummyPlugins);
    ScriptService.initialize(dummyConfig, dummyPlugins);
    SimilarityCreator.initialize(dummyConfig, dummyPlugins);
  }

  private UpdatedFieldInfo initializeAndCheckSimple() {
    FieldAndFacetState initialState = new FieldAndFacetState();
    Map<String, Field> initialFields = Collections.emptyMap();

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialState,
            initialFields,
            simpleUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(2, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(2, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertTrue(fieldAndFacetState.getIndexedAnalyzedFields().isEmpty());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
    return updatedFieldInfo;
  }

  @Test
  public void testRegisterFields() {
    initializeAndCheckSimple();
  }

  @Test
  public void testRegisterAdditionalFields() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.BOOLEAN)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType.FLOAT)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(4, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(4, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof BooleanFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof FloatFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertTrue(fieldAndFacetState.getIndexedAnalyzedFields().isEmpty());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterIdField() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.BOOLEAN)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType._ID)
            .setSearch(true)
            .setStoreDocValues(true)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(4, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(4, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof BooleanFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof IdFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isPresent());
    assertSame(
        fieldAndFacetState.getFields().get("field4"), fieldAndFacetState.getIdFieldDef().get());
    assertTrue(fieldAndFacetState.getIndexedAnalyzedFields().isEmpty());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterSecondIdField() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.BOOLEAN)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType._ID)
            .setSearch(true)
            .setStoreDocValues(true)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));

    try {
      FieldUpdateUtils.updateFields(
          updatedFieldInfo.fieldAndFacetState,
          updatedFieldInfo.fields,
          Collections.singleton(
              Field.newBuilder()
                  .setName("second_id")
                  .setType(FieldType._ID)
                  .setStoreDocValues(true)
                  .build()),
          mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Index can only register one id field, found: field4 and second_id", e.getMessage());
    }
  }

  @Test
  public void testRegisterAnalyzedFields() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.ATOM)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(4, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(4, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof AtomFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof TextFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertEquals(
        Set.of("field3", "field4"), Sets.newHashSet(fieldAndFacetState.getIndexedAnalyzedFields()));
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterVirtualField() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.VIRTUAL)
            .setScript(
                Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("2.0 * field1").build())
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(3, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(3, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof VirtualFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertTrue(fieldAndFacetState.getIndexedAnalyzedFields().isEmpty());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterVirtualFieldRefRequest() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.VIRTUAL)
            .setScript(
                Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("2.0 * field4").build())
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType.DOUBLE)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(4, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(4, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof VirtualFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof DoubleFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertTrue(fieldAndFacetState.getIndexedAnalyzedFields().isEmpty());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterFieldWithChildren() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.OBJECT)
            .setSearch(false)
            .setStoreDocValues(true)
            .addChildFields(
                Field.newBuilder()
                    .setName("child1")
                    .setType(FieldType.ATOM)
                    .setSearch(false)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("child2")
                    .setType(FieldType.DOUBLE)
                    .setSearch(false)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("child3")
                    .setType(FieldType.OBJECT)
                    .setStoreDocValues(true)
                    .addChildFields(
                        Field.newBuilder()
                            .setName("grandchild1")
                            .setType(FieldType.ATOM)
                            .setSearch(false)
                            .setStoreDocValues(true)
                            .build())
                    .build())
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(3, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(7, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof ObjectFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child1") instanceof AtomFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child2") instanceof DoubleFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child3") instanceof ObjectFieldDef);
    assertTrue(
        fieldAndFacetState.getFields().get("field3.child3.grandchild1") instanceof AtomFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertEquals(
        Set.of("field3.child1", "field3.child3.grandchild1"),
        Sets.newHashSet(fieldAndFacetState.getIndexedAnalyzedFields()));
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterFieldWithNestedChildren() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.OBJECT)
            .setSearch(false)
            .setStoreDocValues(true)
            .setNestedDoc(true)
            .addChildFields(
                Field.newBuilder()
                    .setName("child1")
                    .setType(FieldType.ATOM)
                    .setSearch(false)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("child2")
                    .setType(FieldType.DOUBLE)
                    .setSearch(false)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("child3")
                    .setType(FieldType.OBJECT)
                    .setStoreDocValues(true)
                    .addChildFields(
                        Field.newBuilder()
                            .setName("grandchild1")
                            .setType(FieldType.ATOM)
                            .setSearch(false)
                            .setStoreDocValues(true)
                            .build())
                    .build())
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(3, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(7, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof ObjectFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child1") instanceof AtomFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child2") instanceof DoubleFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3.child3") instanceof ObjectFieldDef);
    assertTrue(
        fieldAndFacetState.getFields().get("field3.child3.grandchild1") instanceof AtomFieldDef);
    assertTrue(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertEquals(
        Set.of("field3.child1", "field3.child3.grandchild1"),
        Sets.newHashSet(fieldAndFacetState.getIndexedAnalyzedFields()));
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().isEmpty());
    assertTrue(fieldAndFacetState.getFacetsConfig().getDimConfigs().isEmpty());
    assertTrue(fieldAndFacetState.getInternalFacetFieldNames().isEmpty());
  }

  @Test
  public void testRegisterFieldsWithFacets() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field3")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .setFacet(FacetType.SORTED_SET_DOC_VALUES)
            .setFacetIndexFieldName("$field3")
            .setEagerGlobalOrdinals(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field4")
            .setType(FieldType.FLOAT)
            .setStoreDocValues(true)
            .setMultiValued(false)
            .setSearch(true)
            .setFacet(FacetType.NUMERIC_RANGE)
            .build());

    UpdatedFieldInfo updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            initialInfo.fieldAndFacetState,
            initialInfo.fields,
            fieldUpdates,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Map<String, Field> fields = updatedFieldInfo.fields;
    assertEquals(4, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));

    FieldAndFacetState fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(4, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof TextFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof FloatFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertEquals(List.of("field3"), fieldAndFacetState.getIndexedAnalyzedFields());
    assertEquals(1, fieldAndFacetState.getEagerGlobalOrdinalFields().size());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().containsKey("field3"));
    assertEquals(1, fieldAndFacetState.getFacetsConfig().getDimConfigs().size());
    DimConfig dimConfig = fieldAndFacetState.getFacetsConfig().getDimConfig("field3");
    assertTrue(dimConfig.multiValued);
    assertFalse(dimConfig.hierarchical);
    assertEquals("$field3", dimConfig.indexFieldName);
    assertEquals(Set.of("$field3"), fieldAndFacetState.getInternalFacetFieldNames());

    List<Field> fieldUpdates2 = new ArrayList<>();
    fieldUpdates2.add(
        Field.newBuilder()
            .setName("field5")
            .setType(FieldType.TEXT)
            .setStoreDocValues(true)
            .setMultiValued(false)
            .setFacet(FacetType.HIERARCHY)
            .build());

    updatedFieldInfo =
        FieldUpdateUtils.updateFields(
            updatedFieldInfo.fieldAndFacetState,
            updatedFieldInfo.fields,
            fieldUpdates2,
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    fields = updatedFieldInfo.fields;
    assertEquals(5, fields.size());
    assertEquals(simpleUpdates.get(0), fields.get("field1"));
    assertEquals(simpleUpdates.get(1), fields.get("field2"));
    assertEquals(fieldUpdates.get(0), fields.get("field3"));
    assertEquals(fieldUpdates.get(1), fields.get("field4"));
    assertEquals(fieldUpdates2.get(0), fields.get("field5"));

    fieldAndFacetState = updatedFieldInfo.fieldAndFacetState;
    assertEquals(5, fieldAndFacetState.getFields().size());
    assertTrue(fieldAndFacetState.getFields().get("field1") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field2") instanceof LongFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field3") instanceof TextFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field4") instanceof FloatFieldDef);
    assertTrue(fieldAndFacetState.getFields().get("field5") instanceof TextFieldDef);
    assertFalse(fieldAndFacetState.getHasNestedChildFields());
    assertTrue(fieldAndFacetState.getIdFieldDef().isEmpty());
    assertEquals(Set.of("field3"), Sets.newHashSet(fieldAndFacetState.getIndexedAnalyzedFields()));
    assertEquals(1, fieldAndFacetState.getEagerGlobalOrdinalFields().size());
    assertTrue(fieldAndFacetState.getEagerGlobalOrdinalFields().containsKey("field3"));
    assertEquals(2, fieldAndFacetState.getFacetsConfig().getDimConfigs().size());
    dimConfig = fieldAndFacetState.getFacetsConfig().getDimConfig("field3");
    assertTrue(dimConfig.multiValued);
    assertFalse(dimConfig.hierarchical);
    assertEquals("$field3", dimConfig.indexFieldName);
    dimConfig = fieldAndFacetState.getFacetsConfig().getDimConfig("field5");
    assertFalse(dimConfig.multiValued);
    assertTrue(dimConfig.hierarchical);
    assertEquals("$_field5", dimConfig.indexFieldName);
    assertEquals(Set.of("$field3", "$_field5"), fieldAndFacetState.getInternalFacetFieldNames());
  }

  @Test
  public void testRegisterFieldNameConflict() {
    UpdatedFieldInfo initialInfo = initializeAndCheckSimple();

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field2")
            .setType(FieldType.BOOLEAN)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());

    try {
      FieldUpdateUtils.updateFields(
          initialInfo.fieldAndFacetState,
          initialInfo.fields,
          fieldUpdates,
          mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Duplicate field registration: field2", e.getMessage());
    }
  }

  @Test
  public void testUpdateRequestHandle() throws IOException {
    IndexStateManager mockManager = mock(IndexStateManager.class);

    List<Field> fieldUpdates = new ArrayList<>();
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field1")
            .setType(FieldType.BOOLEAN)
            .setSearch(false)
            .setStoreDocValues(true)
            .build());
    fieldUpdates.add(
        Field.newBuilder()
            .setName("field2")
            .setType(FieldType.FLOAT)
            .setSearch(true)
            .setStoreDocValues(true)
            .setMultiValued(true)
            .build());

    when(mockManager.updateFields(fieldUpdates)).thenReturn("result_string");

    FieldDefRequest request =
        FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fieldUpdates).build();

    String updatedFields = mockManager.updateFields(request.getFieldList());
    FieldDefResponse response = FieldDefResponse.newBuilder().setResponse(updatedFields).build();
    assertEquals("result_string", response.getResponse());

    verify(mockManager, times(1)).updateFields(fieldUpdates);
    verifyNoMoreInteractions(mockManager);
  }
}
