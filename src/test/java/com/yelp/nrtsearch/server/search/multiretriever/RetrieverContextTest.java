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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import java.io.IOException;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

public class RetrieverContextTest {
  private static IndexSearcher searcher() throws IOException {
    return new IndexSearcher(new MultiReader());
  }

  /** Builder with every required field set. */
  private static RetrieverContext.Builder withRequiredFields(String name) {
    return RetrieverContext.newBuilder(name)
        .retrieverType(RetrieverContext.RetrieverType.TEXT)
        .retrieverQuery(new RetrieverQuery(new MatchAllDocsQuery()))
        .docCollector(mock(DocCollector.class));
  }

  @Test
  public void testBuilderDefaults() {
    RetrieverContext context = withRequiredFields("test").build();
    assertEquals("test", context.getName());
    assertEquals(1.0f, context.getBoost(), 0.0f);
    assertNull(context.getRescoreTask());
    assertEquals(0, context.getTopHits());
  }

  @Test
  public void testMissingName() {
    assertThrows(
        NullPointerException.class,
        () ->
            RetrieverContext.newBuilder(null)
                .retrieverType(RetrieverContext.RetrieverType.TEXT)
                .retrieverQuery(new RetrieverQuery(new MatchAllDocsQuery()))
                .docCollector(mock(DocCollector.class))
                .build());
  }

  @Test
  public void testMissingRetrieverType() {
    assertThrows(
        NullPointerException.class, () -> withRequiredFields("test").retrieverType(null).build());
  }

  @Test
  public void testMissingRetrieverQuery() {
    assertThrows(
        NullPointerException.class, () -> withRequiredFields("test").retrieverQuery(null).build());
  }

  @Test
  public void testMissingDocCollector() {
    assertThrows(
        NullPointerException.class, () -> withRequiredFields("test").docCollector(null).build());
  }

  @Test
  public void testBuilderWithAllFields() throws Exception {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    Query query = retrieverQuery.rewrite(searcher());
    DocCollector collector = mock(DocCollector.class);
    RescoreTask rescoreTask = mock(RescoreTask.class);

    RetrieverContext context =
        RetrieverContext.newBuilder("test_retriever")
            .retrieverType(RetrieverContext.RetrieverType.TEXT)
            .boost(2.5f)
            .retrieverQuery(retrieverQuery)
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
  public void testRetrieverQueryHolder() throws Exception {
    Query parsed = new MatchAllDocsQuery();
    RetrieverQuery retrieverQuery = new RetrieverQuery(parsed);
    RetrieverContext context = withRequiredFields("test").retrieverQuery(retrieverQuery).build();

    assertSame(parsed, context.getParsedQuery());
    assertThrows(IllegalStateException.class, context::getQuery);

    Query rewritten = retrieverQuery.rewrite(searcher());
    assertSame(rewritten, context.getRewrittenQuery());
    assertSame(rewritten, context.getQuery());
  }

  @Test
  public void testZeroOrNegativeBoost() {
    RetrieverContext zeroBoost = withRequiredFields("test").boost(0.0f).build();
    assertEquals(1.0f, zeroBoost.getBoost(), 0.0f);

    RetrieverContext negativeBoost = withRequiredFields("test").boost(-1.0f).build();
    assertEquals(1.0f, negativeBoost.getBoost(), 0.0f);
  }
}
