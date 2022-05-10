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
package com.yelp.nrtsearch.server.luceneserver.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

import com.yelp.nrtsearch.server.luceneserver.state.PersistentGlobalState.IndexInfo;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class PersistentGlobalStateTest {

  @Test
  public void testDefaultGlobalState() {
    PersistentGlobalState state = new PersistentGlobalState();
    assertEquals(0, state.getIndices().size());
  }

  @Test
  public void testGlobalState() {
    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo("test_id_1"));
    indicesMap.put("test_index_2", new IndexInfo("test_id_2"));
    PersistentGlobalState state = new PersistentGlobalState(indicesMap);
    assertEquals(indicesMap, state.getIndices());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnmodifiableIndices() {
    PersistentGlobalState state = new PersistentGlobalState();
    state.getIndices().put("test_index", new IndexInfo("test_id_1"));
  }

  @Test
  public void testBuilder() {
    Map<String, IndexInfo> indicesMap = new HashMap<>();
    indicesMap.put("test_index", new IndexInfo("test_id_1"));
    indicesMap.put("test_index_2", new IndexInfo("test_id_2"));
    PersistentGlobalState state = new PersistentGlobalState(indicesMap);

    PersistentGlobalState rebuilt = state.asBuilder().build();
    assertNotSame(state, rebuilt);
    assertEquals(state, rebuilt);

    indicesMap = new HashMap<>();
    indicesMap.put("test_index_3", new IndexInfo("test_id_3"));
    indicesMap.put("test_index_4", new IndexInfo("test_id_4"));
    PersistentGlobalState updated = state.asBuilder().withIndices(indicesMap).build();
    assertNotEquals(state, updated);
    assertEquals(indicesMap, updated.getIndices());
  }

  @Test(expected = NullPointerException.class)
  public void testNullIndices() {
    new PersistentGlobalState(null);
  }
}
