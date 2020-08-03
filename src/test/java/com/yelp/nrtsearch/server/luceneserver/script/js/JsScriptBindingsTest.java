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
package com.yelp.nrtsearch.server.luceneserver.script.js;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefBindings;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

public class JsScriptBindingsTest {

  static class DummyValuesSource extends DoubleValuesSource {

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return null;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return null;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public String toString() {
      return null;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullFieldBindings() {
    new JsScriptBindings(null, Collections.emptyMap());
  }

  @Test(expected = NullPointerException.class)
  public void testNullParams() {
    new JsScriptBindings(new FieldDefBindings(Collections.emptyMap()), null);
  }

  @Test
  public void testGetBindingForParam() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("param1", 100);
    params.put("param2", 1.11);

    JsScriptBindings bindings =
        new JsScriptBindings(new FieldDefBindings(Collections.emptyMap()), params);
    DoubleValuesSource p1Source = bindings.getDoubleValuesSource("param1");
    assertEquals(100D, p1Source.getValues(null, null).doubleValue(), 0.001);
    DoubleValuesSource p2Source = bindings.getDoubleValuesSource("param2");
    assertEquals(1.11, p2Source.getValues(null, null).doubleValue(), 0.001);
  }

  @Test
  public void testGetBindingForField() {
    Map<String, Object> params = new HashMap<>();
    params.put("param1", 100);
    params.put("param2", 1.11);

    DoubleValuesSource field1Source = new DummyValuesSource();
    DoubleValuesSource field2Source = new DummyValuesSource();
    Map<String, FieldDef> fieldDefMap = new HashMap<>();
    fieldDefMap.put("field1", new VirtualFieldDef("field1", field1Source));
    fieldDefMap.put("field2", new VirtualFieldDef("field2", field2Source));

    JsScriptBindings bindings = new JsScriptBindings(new FieldDefBindings(fieldDefMap), params);
    DoubleValuesSource f1Source = bindings.getDoubleValuesSource("field1");
    DoubleValuesSource f2Source = bindings.getDoubleValuesSource("field2");
    assertNotSame(f1Source, f2Source);
    assertSame(f1Source, field1Source);
    assertSame(f2Source, field2Source);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidName() {
    Map<String, Object> params = new HashMap<>();
    params.put("param1", 100);
    params.put("param2", 1.11);

    DoubleValuesSource field1Source = new DummyValuesSource();
    DoubleValuesSource field2Source = new DummyValuesSource();
    Map<String, FieldDef> fieldDefMap = new HashMap<>();
    fieldDefMap.put("field1", new VirtualFieldDef("field1", field1Source));
    fieldDefMap.put("field2", new VirtualFieldDef("field2", field2Source));

    JsScriptBindings bindings = new JsScriptBindings(new FieldDefBindings(fieldDefMap), params);
    bindings.getDoubleValuesSource("invalid");
  }

  @Test
  public void testParamBindingIsCached() {
    Map<String, Object> params = new HashMap<>();
    params.put("param1", 100);
    params.put("param2", 1.11);

    JsScriptBindings bindings =
        new JsScriptBindings(new FieldDefBindings(Collections.emptyMap()), params);
    DoubleValuesSource p1Source = bindings.getDoubleValuesSource("param1");
    DoubleValuesSource p2Source = bindings.getDoubleValuesSource("param2");
    DoubleValuesSource p1SourceNext = bindings.getDoubleValuesSource("param1");
    DoubleValuesSource p2SourceNext = bindings.getDoubleValuesSource("param2");
    assertNotSame(p1Source, p2Source);
    assertSame(p1Source, p1SourceNext);
    assertSame(p2Source, p2SourceNext);
  }
}
