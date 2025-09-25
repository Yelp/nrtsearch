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
package com.yelp.nrtsearch.server.grpc;

import static com.yelp.nrtsearch.server.ServerTestCase.getFieldsFromResourceFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.field.AtomFieldDef;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.LongFieldDef;
import com.yelp.nrtsearch.server.field.TextFieldDef;
import com.yelp.nrtsearch.server.index.ImmutableIndexState;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UpdateFieldsTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static final Set<String> expectedFieldNames =
      Set.of("doc_id", "text_field", "object_field", "int_field");

  private static final Field idField =
      Field.newBuilder()
          .setName("doc_id")
          .setType(FieldType._ID)
          .setSearch(true)
          .setStore(true)
          .build();

  private static final Field textField =
      Field.newBuilder().setName("text_field").setType(FieldType.TEXT).setSearch(true).build();

  private static final Field objectField =
      Field.newBuilder()
          .setName("object_field")
          .setType(FieldType.OBJECT)
          .addChildFields(
              Field.newBuilder()
                  .setName("child_field")
                  .setType(FieldType.ATOM)
                  .setSearch(true)
                  .setStoreDocValues(true)
                  .build())
          .build();

  private static final Field intField =
      Field.newBuilder()
          .setName("int_field")
          .setType(FieldType.INT)
          .setSearch(true)
          .setStoreDocValues(true)
          .build();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer createPrimaryServer() throws Exception {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .build();
    server.createIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.registerFields(
        "test_index", getFieldsFromResourceFile("/registerFieldsUpdateFields.json").getFieldList());
    assertEquals(
        createExpectedFieldMap(), getIndexState(server).getIndexStateInfo().getFieldsMap());
    return server;
  }

  private Map<String, Field> createExpectedFieldMap() {
    Map<String, Field> expectedFieldMap = new HashMap<>();
    expectedFieldMap.put("doc_id", idField);
    expectedFieldMap.put("text_field", textField);
    expectedFieldMap.put("object_field", objectField);
    expectedFieldMap.put("int_field", intField);
    return expectedFieldMap;
  }

  private ImmutableIndexState getIndexState(TestServer server) throws Exception {
    return (ImmutableIndexState) server.getGlobalState().getIndex("test_index");
  }

  @Test
  public void testAddChildField() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("text_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field")
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();
    Field textFieldWithChild =
        textField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field")
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .build();
    expectedFieldMap.put("text_field", textFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field");
    assertTrue(newFieldDef instanceof AtomFieldDef);
  }

  @Test
  public void testAddMultipleChildField() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("text_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_1")
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_2")
                        .setType(FieldType.TEXT)
                        .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                        .setSearch(true)
                        .build())
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();
    Field textFieldWithChild =
        textField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_1")
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_2")
                    .setType(FieldType.TEXT)
                    .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                    .setSearch(true)
                    .build())
            .build();
    expectedFieldMap.put("text_field", textFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field_1");
    assertTrue(newFieldDef instanceof AtomFieldDef);
    newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field_2");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }

  @Test
  public void testAddWithExistingChildren() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("object_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_1")
                        .setType(FieldType.LONG)
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_2")
                        .setType(FieldType.TEXT)
                        .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                        .setSearch(true)
                        .build())
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();
    Field objectFieldWithChild =
        objectField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_1")
                    .setType(FieldType.LONG)
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_2")
                    .setType(FieldType.TEXT)
                    .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                    .setSearch(true)
                    .build())
            .build();
    expectedFieldMap.put("object_field", objectFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef = indexState.getFieldOrThrow("object_field.child_field");
    assertTrue(newFieldDef instanceof AtomFieldDef);
    newFieldDef = indexState.getFieldOrThrow("object_field.new_child_field_1");
    assertTrue(newFieldDef instanceof LongFieldDef);
    newFieldDef = indexState.getFieldOrThrow("object_field.new_child_field_2");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }

  @Test
  public void testAddChildrenToMultipleFields() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("text_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_1")
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_2")
                        .setType(FieldType.TEXT)
                        .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                        .setSearch(true)
                        .build())
                .build(),
            Field.newBuilder()
                .setName("object_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_1")
                        .setType(FieldType.LONG)
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field_2")
                        .setType(FieldType.TEXT)
                        .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                        .setSearch(true)
                        .build())
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();
    Field textFieldWithChild =
        textField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_1")
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_2")
                    .setType(FieldType.TEXT)
                    .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                    .setSearch(true)
                    .build())
            .build();
    expectedFieldMap.put("text_field", textFieldWithChild);

    Field objectFieldWithChild =
        objectField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_1")
                    .setType(FieldType.LONG)
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field_2")
                    .setType(FieldType.TEXT)
                    .setAnalyzer(Analyzer.newBuilder().setPredefined("classic").build())
                    .setSearch(true)
                    .build())
            .build();
    expectedFieldMap.put("object_field", objectFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field_1");
    assertTrue(newFieldDef instanceof AtomFieldDef);
    newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field_2");
    assertTrue(newFieldDef instanceof TextFieldDef);

    newFieldDef = indexState.getFieldOrThrow("object_field.child_field");
    assertTrue(newFieldDef instanceof AtomFieldDef);
    newFieldDef = indexState.getFieldOrThrow("object_field.new_child_field_1");
    assertTrue(newFieldDef instanceof LongFieldDef);
    newFieldDef = indexState.getFieldOrThrow("object_field.new_child_field_2");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }

  @Test
  public void testAddChildAndTopLevelField() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("text_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field")
                        .setSearch(true)
                        .setStoreDocValues(true)
                        .build())
                .build(),
            Field.newBuilder()
                .setName("new_top_level_field")
                .setType(FieldType.TEXT)
                .setSearch(true)
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();
    Field textFieldWithChild =
        textField.toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field")
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .build();
    expectedFieldMap.put("text_field", textFieldWithChild);
    expectedFieldMap.put(
        "new_top_level_field",
        Field.newBuilder()
            .setName("new_top_level_field")
            .setType(FieldType.TEXT)
            .setSearch(true)
            .build());
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef = indexState.getFieldOrThrow("text_field.new_child_field");
    assertTrue(newFieldDef instanceof AtomFieldDef);
    newFieldDef = indexState.getFieldOrThrow("new_top_level_field");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }

  @Test
  public void testAddNestedChildField() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("object_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("child_field")
                        .addChildFields(
                            Field.newBuilder()
                                .setName("nested_child_field")
                                .setType(FieldType.TEXT)
                                .setSearch(true)
                                .setStoreDocValues(true)
                                .build()))
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();

    Field nestedChildField =
        objectField.getChildFields(0).toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("nested_child_field")
                    .setType(FieldType.TEXT)
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .build();

    Field objectFieldWithChild =
        objectField.toBuilder().setChildFields(0, nestedChildField).build();
    expectedFieldMap.put("object_field", objectFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef =
        indexState.getFieldOrThrow("object_field.child_field.nested_child_field");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }

  @Test
  public void testAddNestedChildFieldToMultipleLevels() throws Exception {
    TestServer primaryServer = createPrimaryServer();
    primaryServer.registerFields(
        "test_index",
        List.of(
            Field.newBuilder()
                .setName("object_field")
                .addChildFields(
                    Field.newBuilder()
                        .setName("child_field")
                        .addChildFields(
                            Field.newBuilder()
                                .setName("nested_child_field")
                                .setType(FieldType.TEXT)
                                .setSearch(true)
                                .setStoreDocValues(true)
                                .build()))
                .addChildFields(
                    Field.newBuilder()
                        .setName("new_child_field")
                        .setType(FieldType.TEXT)
                        .setSearch(true)
                        .setStore(true)
                        .build())
                .build()));
    ImmutableIndexState indexState = getIndexState(primaryServer);
    Map<String, Field> fieldMap = indexState.getIndexStateInfo().getFieldsMap();
    assertEquals(expectedFieldNames, fieldMap.keySet());

    Map<String, Field> expectedFieldMap = createExpectedFieldMap();

    Field nestedChildField =
        objectField.getChildFields(0).toBuilder()
            .addChildFields(
                Field.newBuilder()
                    .setName("nested_child_field")
                    .setType(FieldType.TEXT)
                    .setSearch(true)
                    .setStoreDocValues(true)
                    .build())
            .build();

    Field objectFieldWithChild =
        objectField.toBuilder()
            .setChildFields(0, nestedChildField)
            .addChildFields(
                Field.newBuilder()
                    .setName("new_child_field")
                    .setType(FieldType.TEXT)
                    .setSearch(true)
                    .setStore(true)
                    .build())
            .build();
    expectedFieldMap.put("object_field", objectFieldWithChild);
    assertEquals(expectedFieldMap, indexState.getIndexStateInfo().getFieldsMap());

    FieldDef newFieldDef =
        indexState.getFieldOrThrow("object_field.child_field.nested_child_field");
    assertTrue(newFieldDef instanceof TextFieldDef);
    newFieldDef = indexState.getFieldOrThrow("object_field.new_child_field");
    assertTrue(newFieldDef instanceof TextFieldDef);
  }
}
