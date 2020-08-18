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

import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.plugins.FieldTypePlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
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

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  static class TestFieldDef extends FieldDef {

    public TestFieldDef(String name, Field requestField) {
      super(name);
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
    Field field = Field.newBuilder().build();
    FieldDefCreator.getInstance().createFieldDef("test_field", "custom_field_type", field);
  }

  @Test
  public void testPluginProvidesFieldType() {
    init(Collections.singletonList(new TestFieldTypePlugin()));
    Field field = Field.newBuilder().build();
    FieldDef testFieldDef =
        FieldDefCreator.getInstance().createFieldDef("test_field", "custom_field_type", field);
    assertTrue(testFieldDef instanceof TestFieldDef);
  }
}
