/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.script.js;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.doc.DefaultSharedDocContext;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests for JS variable resolution of shared doc context values. Retriever scores are stored in
 * SharedDocContext under the key {@code retriever_<name>} and accessed in JS expressions as {@code
 * _shared_retriever_<name>}.
 */
@RunWith(MockitoJUnitRunner.class)
public class JsScriptSideValuesTest {

  /** A no-op Bindings that throws for any lookup to confirm shared-context bypass field lookup. */
  private static final Bindings EMPTY_BINDINGS =
      new Bindings() {
        @Override
        public DoubleValuesSource getDoubleValuesSource(String name) {
          throw new IllegalArgumentException("Unexpected field lookup: " + name);
        }
      };

  private static LeafReaderContext leafWithBase(int docBase) {
    LeafReaderContext ctx = mock(LeafReaderContext.class);
    try {
      java.lang.reflect.Field f = LeafReaderContext.class.getField("docBase");
      f.setAccessible(true);
      f.set(ctx, docBase);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ctx;
  }

  @Test
  public void testRetrieverScoreResolvesViaJsVariable() throws IOException {
    SharedDocContext ctx = new DefaultSharedDocContext();
    ctx.getContext(0).put("retriever_text", 7.25);
    JsScriptBindings bindings = new JsScriptBindings(EMPTY_BINDINGS, Collections.emptyMap(), ctx);

    DoubleValuesSource source = bindings.getDoubleValuesSource("_shared_retriever_text");
    assertNotNull(source);

    LeafReaderContext leaf = leafWithBase(0);
    DoubleValues values = source.getValues(leaf, null);
    assertTrue(values.advanceExact(0)); // doc present → true
    assertEquals(7.25, values.doubleValue(), 0.0001);
  }

  @Test
  public void testAdvanceExactReturnsFalseWhenDocMissing() throws IOException {
    SharedDocContext ctx = new DefaultSharedDocContext();
    ctx.getContext(0).put("retriever_text", 5.0); // only doc 0
    JsScriptBindings bindings = new JsScriptBindings(EMPTY_BINDINGS, Collections.emptyMap(), ctx);

    DoubleValuesSource source = bindings.getDoubleValuesSource("_shared_retriever_text");
    LeafReaderContext leaf = leafWithBase(0);
    DoubleValues values = source.getValues(leaf, null);
    assertFalse(values.advanceExact(99)); // doc 99 not present → false
  }

  @Test
  public void testSharedContextValueUsesLeafDocBase() throws IOException {
    // globalDoc=10: leafBase=10, segmentDoc=0
    SharedDocContext ctx = new DefaultSharedDocContext();
    ctx.getContext(10).put("retriever_knn", 3.5);
    JsScriptBindings bindings = new JsScriptBindings(EMPTY_BINDINGS, Collections.emptyMap(), ctx);

    DoubleValuesSource source = bindings.getDoubleValuesSource("_shared_retriever_knn");
    LeafReaderContext leaf = leafWithBase(10);
    DoubleValues values = source.getValues(leaf, null);
    assertTrue(values.advanceExact(0)); // segmentDoc=0 → globalDoc=10
    assertEquals(3.5, values.doubleValue(), 0.0001);
  }

  @Test
  public void testNonSharedPrefixDelegatesNormally() {
    JsScriptBindings bindings =
        new JsScriptBindings(EMPTY_BINDINGS, Map.of("multiplier", 2.0), null);
    DoubleValuesSource source = bindings.getDoubleValuesSource("multiplier");
    assertNotNull(source);
  }

  @Test
  public void testSharedPrefixWithNullContextReturnsFalse() throws IOException {
    // null sharedDocContext → advanceExact always returns false
    JsScriptBindings bindings = new JsScriptBindings(EMPTY_BINDINGS, Collections.emptyMap(), null);

    DoubleValuesSource source = bindings.getDoubleValuesSource("_shared_retriever_anything");
    assertNotNull(source);
    LeafReaderContext leaf = leafWithBase(0);
    DoubleValues values = source.getValues(leaf, null);
    assertFalse(values.advanceExact(0));
  }
}
