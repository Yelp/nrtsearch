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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

public class RetrieverContextTest {

  @Test
  public void testBuilderDefaults() {
    RetrieverContext context = RetrieverContext.newBuilder("test").build();
    assertEquals("test", context.getName());
    assertNull(context.getRetrieverType());
    assertEquals(1.0f, context.getBoost(), 0.0f);
    assertNull(context.getQuery());
    assertNull(context.getDocCollector());
    assertNull(context.getRescoreTask());
  }

  @Test
  public void testBuilderWithAllFields() {
    Query query = new MatchAllDocsQuery();
    DocCollector collector = mock(DocCollector.class);
    RescoreTask rescoreTask = mock(RescoreTask.class);

    RetrieverContext context =
        RetrieverContext.newBuilder("test_retriever")
            .retrieverType(RetrieverContext.RetrieverType.TEXT)
            .boost(2.5f)
            .query(query)
            .docCollector(collector)
            .rescoreTask(rescoreTask)
            .build();

    assertEquals("test_retriever", context.getName());
    assertEquals(RetrieverContext.RetrieverType.TEXT, context.getRetrieverType());
    assertEquals(2.5f, context.getBoost(), 0.0f);
    assertSame(query, context.getQuery());
    assertSame(collector, context.getDocCollector());
    assertSame(rescoreTask, context.getRescoreTask());
  }

  @Test
  public void testZeroOrNegativeBoost() {
    RetrieverContext zeroBoost = RetrieverContext.newBuilder("test").boost(0.0f).build();
    assertEquals(1.0f, zeroBoost.getBoost(), 0.0f);

    RetrieverContext negativeBoost = RetrieverContext.newBuilder("test").boost(-1.0f).build();
    assertEquals(1.0f, negativeBoost.getBoost(), 0.0f);
  }
}
