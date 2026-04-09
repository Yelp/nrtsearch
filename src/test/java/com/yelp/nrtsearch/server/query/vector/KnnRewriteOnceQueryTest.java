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
package com.yelp.nrtsearch.server.query.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.query.MinThresholdQuery;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.junit.Test;

public class KnnRewriteOnceQueryTest {

  /**
   * Fake KNN query that counts how many times rewrite() is called and returns a fixed sentinel
   * query as its rewritten form. Implements {@link WithVectorTotalHits} so it passes the
   * constructor validation in {@link KnnRewriteOnceQuery}.
   */
  private static class CountingKnnQuery extends Query implements WithVectorTotalHits {
    final AtomicInteger rewriteCount = new AtomicInteger();
    final Query rewriteResult;

    CountingKnnQuery(Query rewriteResult) {
      this.rewriteResult = rewriteResult;
    }

    @Override
    public TotalHits getTotalHits() {
      return null;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) {
      rewriteCount.incrementAndGet();
      return rewriteResult;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
      return "CountingKnnQuery";
    }

    @Override
    public boolean equals(Object o) {
      return sameClassAs(o);
    }

    @Override
    public int hashCode() {
      return classHash();
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor validation
  // ---------------------------------------------------------------------------

  @Test
  public void testConstructorAcceptsWithVectorTotalHits() {
    // CountingKnnQuery implements WithVectorTotalHits — should not throw
    new KnnRewriteOnceQuery(new CountingKnnQuery(new MatchAllDocsQuery()));
  }

  @Test
  public void testConstructorAcceptsMinThresholdWrappingKnnQuery() {
    Query knn = new CountingKnnQuery(new MatchAllDocsQuery());
    new KnnRewriteOnceQuery(new MinThresholdQuery(knn, 0.5f));
  }

  @Test
  public void testConstructorRejectsNonKnnQuery() {
    assertThrows(
        IllegalArgumentException.class, () -> new KnnRewriteOnceQuery(new MatchAllDocsQuery()));
  }

  @Test
  public void testConstructorRejectsMinThresholdWrappingNonKnnQuery() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new KnnRewriteOnceQuery(new MinThresholdQuery(new MatchAllDocsQuery(), 0.5f)));
  }

  // ---------------------------------------------------------------------------
  // Core caching behaviour
  // ---------------------------------------------------------------------------

  @Test
  public void testRewriteCalledOnlyOnce() throws IOException {
    Query sentinel = new MatchAllDocsQuery();
    CountingKnnQuery inner = new CountingKnnQuery(sentinel);
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    Query r1 = wrapper.rewrite(null);
    Query r2 = wrapper.rewrite(null);
    Query r3 = wrapper.rewrite(null);

    assertEquals(1, inner.rewriteCount.get());
    assertSame(sentinel, r1);
    assertSame(r1, r2);
    assertSame(r1, r3);
  }

  @Test
  public void testRewriteReturnsCachedResultNotThis() throws IOException {
    Query sentinel = new MatchAllDocsQuery();
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(new CountingKnnQuery(sentinel));

    assertSame(sentinel, wrapper.rewrite(null));
  }

  // ---------------------------------------------------------------------------
  // getWrapped
  // ---------------------------------------------------------------------------

  @Test
  public void testGetWrappedReturnsOriginalQuery() {
    CountingKnnQuery inner = new CountingKnnQuery(new MatchAllDocsQuery());
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    assertSame(inner, wrapper.getWrapped());
  }

  // ---------------------------------------------------------------------------
  // equals / hashCode
  // ---------------------------------------------------------------------------

  @Test
  public void testEqualWhenSameInner() {
    CountingKnnQuery inner = new CountingKnnQuery(new MatchAllDocsQuery());
    KnnRewriteOnceQuery a = new KnnRewriteOnceQuery(inner);
    KnnRewriteOnceQuery b = new KnnRewriteOnceQuery(inner);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testNotEqualWhenDifferentInner() {
    // Use two distinct CountingKnnQuery instances — CountingKnnQuery.equals uses sameClassAs
    // so two instances of the same anonymous subclass are equal; use a named override to break
    // that.
    KnnRewriteOnceQuery a = new KnnRewriteOnceQuery(new CountingKnnQuery(new MatchAllDocsQuery()));
    KnnRewriteOnceQuery b =
        new KnnRewriteOnceQuery(
            new CountingKnnQuery(new MatchAllDocsQuery()) {
              @Override
              public boolean equals(Object o) {
                return false;
              }

              @Override
              public int hashCode() {
                return System.identityHashCode(this);
              }
            });

    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualToDifferentType() {
    KnnRewriteOnceQuery wrapper =
        new KnnRewriteOnceQuery(new CountingKnnQuery(new MatchAllDocsQuery()));
    assertNotEquals(wrapper, new MatchAllDocsQuery());
  }

  // ---------------------------------------------------------------------------
  // toString
  // ---------------------------------------------------------------------------

  @Test
  public void testToStringContainsInnerQuery() {
    MatchAllDocsQuery inner = new MatchAllDocsQuery();
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    String s = wrapper.toString("field");
    assertEquals("KnnRewriteOnce(" + inner.toString("field") + ")", s);
  }

  // ---------------------------------------------------------------------------
  // Searcher-keyed cache invalidation
  // ---------------------------------------------------------------------------

  @Test
  public void testSameSearcherHitsCacheOnSubsequentCalls() throws IOException {
    // Two distinct sentinel results so we can tell which searcher's result we got.
    Query sentinel1 = new MatchAllDocsQuery();
    CountingKnnQuery inner = new CountingKnnQuery(sentinel1);
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    IndexSearcher searcher = mock(IndexSearcher.class);

    Query r1 = wrapper.rewrite(searcher);
    Query r2 = wrapper.rewrite(searcher);
    Query r3 = wrapper.rewrite(searcher);

    assertEquals(1, inner.rewriteCount.get());
    assertSame(sentinel1, r1);
    assertSame(r1, r2);
    assertSame(r1, r3);
  }

  @Test
  public void testDifferentSearcherInvalidatesCache() throws IOException {
    Query sentinel1 = new MatchAllDocsQuery();
    Query sentinel2 = new MatchAllDocsQuery();

    // Inner alternates return value on each rewrite call.
    CountingKnnQuery inner =
        new CountingKnnQuery(sentinel1) {
          @Override
          public Query rewrite(IndexSearcher searcher) {
            return rewriteCount.incrementAndGet() == 1 ? rewriteResult : sentinel2;
          }
        };
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    IndexSearcher searcher1 = mock(IndexSearcher.class);
    IndexSearcher searcher2 = mock(IndexSearcher.class);

    Query r1 = wrapper.rewrite(searcher1);
    Query r2 = wrapper.rewrite(searcher1); // same searcher — cached
    Query r3 = wrapper.rewrite(searcher2); // different searcher — re-executes

    assertEquals(2, inner.rewriteCount.get());
    assertSame(sentinel1, r1);
    assertSame(sentinel1, r2); // still sentinel1, from cache
    assertSame(sentinel2, r3); // sentinel2, from fresh execution
  }

  @Test
  public void testSwitchingBackToOriginalSearcherReExecutes() throws IOException {
    // Each rewrite call returns a new MatchAllDocsQuery instance so we can track them.
    Query[] sentinels = {new MatchAllDocsQuery(), new MatchAllDocsQuery(), new MatchAllDocsQuery()};
    Query inner =
        new CountingKnnQuery(null) {
          @Override
          public Query rewrite(IndexSearcher s) {
            return sentinels[rewriteCount.getAndIncrement()];
          }
        };
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);

    IndexSearcher s1 = mock(IndexSearcher.class);
    IndexSearcher s2 = mock(IndexSearcher.class);

    assertSame(sentinels[0], wrapper.rewrite(s1));
    assertSame(sentinels[0], wrapper.rewrite(s1)); // cached for s1
    assertSame(sentinels[1], wrapper.rewrite(s2)); // new searcher — re-executes
    assertSame(sentinels[1], wrapper.rewrite(s2)); // cached for s2
    assertSame(sentinels[2], wrapper.rewrite(s1)); // back to s1 — re-executes again
  }

  // ---------------------------------------------------------------------------
  // Thread safety: concurrent first-time rewrites for the same searcher
  // ---------------------------------------------------------------------------

  @Test
  public void testConcurrentRewriteOnlyExecutesOnce() throws Exception {
    Query sentinel = new MatchAllDocsQuery();
    CountingKnnQuery inner = new CountingKnnQuery(sentinel);
    KnnRewriteOnceQuery wrapper = new KnnRewriteOnceQuery(inner);
    IndexSearcher searcher = mock(IndexSearcher.class);

    int threads = 8;
    Thread[] pool = new Thread[threads];
    Query[] results = new Query[threads];

    for (int i = 0; i < threads; i++) {
      final int idx = i;
      pool[i] =
          new Thread(
              () -> {
                try {
                  results[idx] = wrapper.rewrite(searcher);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
    for (Thread t : pool) t.start();
    for (Thread t : pool) t.join();

    assertEquals(1, inner.rewriteCount.get());
    for (Query r : results) {
      assertSame(sentinel, r);
    }
  }
}
