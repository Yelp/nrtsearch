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
package com.yelp.nrtsearch.server.search.multiretriever;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Test;

public class MultiRetrieverContextTest {

  private static RetrieverContext buildRetriever(String name) {
    return RetrieverContext.newBuilder(name).query(new MatchAllDocsQuery()).build();
  }

  @Test
  public void testSingleRetriever() {
    RetrieverContext retriever = buildRetriever("text");
    MultiRetrieverContext context =
        MultiRetrieverContext.newBuilder().addRetrieverContext(retriever).build();

    assertSame(retriever, context.getRetrieverContext("text"));
    assertEquals(1, context.getRetrieverContextMap().size());
  }

  @Test
  public void testMultiRetriever() {
    RetrieverContext text = buildRetriever("text");
    RetrieverContext knn = buildRetriever("knn");
    MultiRetrieverContext context =
        MultiRetrieverContext.newBuilder()
            .addRetrieverContext(text)
            .addRetrieverContext(knn)
            .build();

    assertSame(text, context.getRetrieverContext("text"));
    assertSame(knn, context.getRetrieverContext("knn"));
    assertEquals(2, context.getRetrieverContextMap().size());
  }

  @Test
  public void testRetrieverOrderPreserved() {
    RetrieverContext first = buildRetriever("first");
    RetrieverContext second = buildRetriever("second");
    RetrieverContext third = buildRetriever("third");
    MultiRetrieverContext context =
        MultiRetrieverContext.newBuilder()
            .addRetrieverContext(first)
            .addRetrieverContext(second)
            .addRetrieverContext(third)
            .build();

    String[] keys = context.getRetrieverContextMap().keySet().toArray(new String[0]);
    assertEquals("first", keys[0]);
    assertEquals("second", keys[1]);
    assertEquals("third", keys[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateNameThrowsException() {
    MultiRetrieverContext.newBuilder()
        .addRetrieverContext(buildRetriever("text"))
        .addRetrieverContext(buildRetriever("text"))
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyThrows() {
    MultiRetrieverContext.newBuilder().build();
  }
}
