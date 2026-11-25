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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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

public class RetrieverQueryTest {
  private static IndexSearcher searcher() throws IOException {
    return new IndexSearcher(new MultiReader());
  }

  @Test
  public void testParsedQuery() {
    Query parsed = new MatchAllDocsQuery();
    assertSame(parsed, new RetrieverQuery(parsed).getParsedQuery());
  }

  @Test
  public void testNullParsedQuery() {
    assertThrows(NullPointerException.class, () -> new RetrieverQuery(null));
  }

  @Test
  public void testGetRewrittenQueryBeforeRewrite() {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    assertFalse(retrieverQuery.isRewritten());
    assertThrows(IllegalStateException.class, retrieverQuery::getRewrittenQuery);
  }

  @Test
  public void testRewriteThenGet() throws Exception {
    Query inner = new TermQuery(new Term("f", "a"));
    // A single clause boolean query rewrites to its clause, so parsed and rewritten differ
    Query parsed = new BooleanQuery.Builder().add(inner, BooleanClause.Occur.SHOULD).build();
    RetrieverQuery retrieverQuery = new RetrieverQuery(parsed);
    Query rewritten = retrieverQuery.rewrite(searcher());
    assertEquals(inner, rewritten);
    assertNotSame(parsed, rewritten);
    assertTrue(retrieverQuery.isRewritten());
    assertSame(rewritten, retrieverQuery.getRewrittenQuery());
    assertSame(parsed, retrieverQuery.getParsedQuery());
  }

  @Test
  public void testRewriteTwice() throws Exception {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    retrieverQuery.rewrite(searcher());
    assertThrows(IllegalStateException.class, () -> retrieverQuery.rewrite(searcher()));
  }

  @Test
  public void testRewriteConcurrently() throws Exception {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new TermQuery(new Term("f", "a")));
    IndexSearcher indexSearcher = searcher();
    int threads = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Query>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return retrieverQuery.rewrite(indexSearcher);
                }));
      }
      start.countDown();
      int successes = 0;
      int failures = 0;
      for (Future<Query> result : results) {
        try {
          result.get();
          successes++;
        } catch (java.util.concurrent.ExecutionException e) {
          assertSame(IllegalStateException.class, e.getCause().getClass());
          failures++;
        }
      }
      assertEquals(1, successes);
      assertEquals(threads - 1, failures);
    } finally {
      executor.shutdownNow();
    }
    assertTrue(retrieverQuery.isRewritten());
  }

  @Test
  public void testRewriteNullSearcher() {
    RetrieverQuery retrieverQuery = new RetrieverQuery(new MatchAllDocsQuery());
    assertThrows(NullPointerException.class, () -> retrieverQuery.rewrite(null));
  }
}
