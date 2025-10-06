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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.plugins.FieldTypePlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class FieldDefCreatorTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    FieldDefCreator.initialize(getEmptyConfig(), plugins);
  }

  private NrtsearchConfig getEmptyConfig() {
    String config = "nodeName: \"server_foo\"";
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  static class TestFieldDef extends FieldDef {
    final FieldDefCreator.FieldDefCreatorContext context;

    public TestFieldDef(
        String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
      super(name);
      this.context = context;
    }

    @Override
    public String getType() {
      return "custom_field_type";
    }

    /**
     * Get the facet value type for this field.
     *
     * @return field facet value type
     */
    @Override
    public IndexableFieldDef.FacetValueType getFacetValueType() {
      return IndexableFieldDef.FacetValueType.NO_FACETS;
    }
  }

  static class TestFieldTypePlugin extends Plugin implements FieldTypePlugin {
    @Override
    public Map<String, FieldDefProvider<? extends FieldDef>> getFieldTypes() {
      Map<String, FieldDefProvider<? extends FieldDef>> typeMap = new HashMap<>();
      typeMap.put("custom_field_type", TestFieldDef::new);
      return typeMap;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCustomFieldNotDefined() {
    Field field =
        Field.newBuilder()
            .setType(FieldType.CUSTOM)
            .setAdditionalProperties(
                Struct.newBuilder()
                    .putFields(
                        "type", Value.newBuilder().setStringValue("custom_field_type").build())
                    .build())
            .build();
    FieldDefCreator.getInstance()
        .createFieldDef("test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  @Test
  public void testPluginProvidesFieldType() {
    init(Collections.singletonList(new TestFieldTypePlugin()));
    Field field =
        Field.newBuilder()
            .setType(FieldType.CUSTOM)
            .setAdditionalProperties(
                Struct.newBuilder()
                    .putFields(
                        "type", Value.newBuilder().setStringValue("custom_field_type").build())
                    .build())
            .build();
    FieldDef testFieldDef =
        FieldDefCreator.getInstance()
            .createFieldDef(
                "test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
    assertTrue(testFieldDef instanceof TestFieldDef);
  }

  @Test
  public void testProvidesContext() {
    init(Collections.singletonList(new TestFieldTypePlugin()));
    Field field =
        Field.newBuilder()
            .setType(FieldType.CUSTOM)
            .setAdditionalProperties(
                Struct.newBuilder()
                    .putFields(
                        "type", Value.newBuilder().setStringValue("custom_field_type").build())
                    .build())
            .build();
    FieldDefCreator.FieldDefCreatorContext mockContext =
        mock(FieldDefCreator.FieldDefCreatorContext.class);
    FieldDef testFieldDef =
        FieldDefCreator.getInstance().createFieldDef("test_field", field, mockContext);
    assertTrue(testFieldDef instanceof TestFieldDef);
    assertSame(mockContext, ((TestFieldDef) testFieldDef).context);
  }

  @Test
  public void testCreateContext() {
    NrtsearchConfig config = getEmptyConfig();
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getConfiguration()).thenReturn(config);
    FieldDefCreator.FieldDefCreatorContext context = FieldDefCreator.createContext(mockGlobalState);

    assertSame(config, context.config());
  }

  @Test
  public void testCreateFieldDefFromPreviousNullUpdatedField() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    when(mockFieldDef.getName()).thenReturn("test_field");
    try {
      FieldDefCreator.getInstance()
          .createFieldDefFromPrevious(
              "field",
              Field.newBuilder().build(),
              mockFieldDef,
              mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("FieldDef test_field cannot be updated", e.getMessage());
    }
  }
}
