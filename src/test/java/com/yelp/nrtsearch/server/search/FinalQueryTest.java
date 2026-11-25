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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import com.yelp.nrtsearch.server.search.multiretriever.MultiRetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverContext;
import com.yelp.nrtsearch.server.search.multiretriever.RetrieverQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

public class FinalQueryTest {

  private static IndexSearcher searcher() throws IOException {
    return new IndexSearcher(new MultiReader());
  }

  private static MultiRetrieverContext contextOf(RetrieverQuery... retrieverQueries) {
    MultiRetrieverContext.Builder builder = MultiRetrieverContext.newBuilder();
    for (int i = 0; i < retrieverQueries.length; ++i) {
      builder.addRetrieverContext(
          RetrieverContext.newBuilder("r" + i)
              .retrieverType(RetrieverContext.RetrieverType.TEXT)
              .retrieverQuery(retrieverQueries[i])
              .docCollector(mock(DocCollector.class))
              .build());
    }
    return builder.build();
  }

  @Test
  public void testOf() {
    Query query = new MatchAllDocsQuery();
    assertSame(query, FinalQuery.of(query).get());
  }

  @Test
  public void testOfNull() {
    assertThrows(NullPointerException.class, () -> FinalQuery.of(null));
  }

  @Test
  public void testUnionBeforeRetrieversRewritten() throws Exception {
    RetrieverQuery first = new RetrieverQuery(new MatchAllDocsQuery());
    RetrieverQuery second = new RetrieverQuery(new MatchAllDocsQuery());
    FinalQuery finalQuery = FinalQuery.unionOf(contextOf(first, second));

    assertThrows(IllegalStateException.class, finalQuery::get);
    first.rewrite(searcher());
    assertThrows(IllegalStateException.class, finalQuery::get);
  }

  @Test
  public void testUnionInRequestOrder() throws Exception {
    Query firstQuery = new TermQuery(new Term("f", "a"));
    Query secondQuery = new TermQuery(new Term("f", "b"));
    // A single clause boolean query rewrites to its clause, so the union must use the rewritten
    // queries, not the parsed ones
    RetrieverQuery first =
        new RetrieverQuery(
            new BooleanQuery.Builder().add(firstQuery, BooleanClause.Occur.SHOULD).build());
    RetrieverQuery second =
        new RetrieverQuery(
            new BooleanQuery.Builder().add(secondQuery, BooleanClause.Occur.SHOULD).build());
    first.rewrite(searcher());
    second.rewrite(searcher());

    Query union = FinalQuery.unionOf(contextOf(first, second)).get();

    Query expected =
        new BooleanQuery.Builder()
            .add(firstQuery, BooleanClause.Occur.SHOULD)
            .add(secondQuery, BooleanClause.Occur.SHOULD)
            .build();
    assertEquals(expected, union);
  }

  @Test
  public void testUnionBuiltOnce() throws Exception {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    retrieverQuery.rewrite(searcher());
    FinalQuery finalQuery = FinalQuery.unionOf(contextOf(retrieverQuery));

    Query first = finalQuery.get();
    assertSame(first, finalQuery.get());
  }

  @Test
  public void testUnionBuiltOnceConcurrently() throws Exception {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    retrieverQuery.rewrite(searcher());
    FinalQuery finalQuery = FinalQuery.unionOf(contextOf(retrieverQuery));

    int threads = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Query>> futures = new ArrayList<>();
      for (int i = 0; i < threads; ++i) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  return finalQuery.get();
                }));
      }
      start.countDown();
      Query first = futures.get(0).get();
      for (Future<Query> future : futures) {
        assertSame(first, future.get());
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
