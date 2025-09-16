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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.KnnQuery;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.junit.Test;

public class VectorFieldDefCreateKnnSearchStrategyTest {

  @Test
  public void testCreateKnnSearchStrategy_Default() {
    KnnSearchStrategy strategy =
        VectorFieldDef.createKnnSearchStrategy(KnnQuery.FilterStrategy.DEFAULT);
    assertNotNull(strategy);
    assertEquals(KnnSearchStrategy.Hnsw.DEFAULT, strategy);
  }

  @Test
  public void testCreateKnnSearchStrategy_Acorn() {
    KnnSearchStrategy strategy =
        VectorFieldDef.createKnnSearchStrategy(KnnQuery.FilterStrategy.ACORN);
    assertNotNull(strategy);
    assertEquals(KnnSearchStrategy.Hnsw.class, strategy.getClass());
    // Since we can't directly check the internal value, we verify it's a different instance from
    // DEFAULT
    KnnSearchStrategy defaultStrategy = KnnSearchStrategy.Hnsw.DEFAULT;
    assertNotEquals(defaultStrategy, strategy);
  }

  @Test
  public void testCreateKnnSearchStrategy_Unknown() {
    try {
      VectorFieldDef.createKnnSearchStrategy(KnnQuery.FilterStrategy.UNRECOGNIZED);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("Unknown filter strategy: UNRECOGNIZED", e.getMessage());
    }
  }

  // Helper method to compare objects for inequality
  private static void assertNotEquals(Object expected, Object actual) {
    if (expected == null ? actual == null : expected.equals(actual)) {
      fail("Expected objects to be different, but they are equal");
    }
  }
}
