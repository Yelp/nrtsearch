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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics.VectorDiagnostics;
import com.yelp.nrtsearch.server.query.MinThresholdQuery;
import com.yelp.nrtsearch.server.query.vector.WithVectorTotalHits;
import java.io.IOException;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;

public class KnnUtilsTest {
  private IndexSearcher indexSearcher;

  @Before
  public void setUp() throws IOException {
    indexSearcher = new IndexSearcher(new MultiReader());
  }

  /** Vector query stub that rewrites to a fixed query without populating total hits. */
  private static class NullTotalHitsVectorQuery extends Query implements WithVectorTotalHits {
    private final Query rewriteTo;

    NullTotalHitsVectorQuery(Query rewriteTo) {
      this.rewriteTo = rewriteTo;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) {
      return rewriteTo;
    }

    @Override
    public TotalHits getTotalHits() {
      return null;
    }

    @Override
    public String toString(String field) {
      return "NullTotalHitsVectorQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  private void assertZeroTotalHits(VectorDiagnostics vectorDiagnostics) {
    assertEquals(0, vectorDiagnostics.getTotalHits().getValue());
    assertEquals(
        com.yelp.nrtsearch.server.grpc.TotalHits.Relation.EQUAL_TO,
        vectorDiagnostics.getTotalHits().getRelation());
  }

  @Test
  public void testResolveKnnQuery_matchNoDocsRewrite_zeroTotalHits() throws IOException {
    VectorDiagnostics.Builder builder = VectorDiagnostics.newBuilder();
    Query resolved =
        KnnUtils.resolveKnnQueryAndBoost(
            new NullTotalHitsVectorQuery(new MatchNoDocsQuery()), 1.0f, indexSearcher, builder);
    assertTrue(resolved instanceof MatchNoDocsQuery);
    assertZeroTotalHits(builder.build());
  }

  @Test
  public void testResolveKnnQuery_minThresholdMatchNoDocsRewrite_zeroTotalHits()
      throws IOException {
    VectorDiagnostics.Builder builder = VectorDiagnostics.newBuilder();
    KnnUtils.resolveKnnQueryAndBoost(
        new MinThresholdQuery(new NullTotalHitsVectorQuery(new MatchNoDocsQuery()), 0.5f),
        1.0f,
        indexSearcher,
        builder);
    assertZeroTotalHits(builder.build());
  }

  @Test
  public void testResolveKnnQuery_nullTotalHits_throws() {
    Query knnQuery = new NullTotalHitsVectorQuery(new MatchAllDocsQuery());
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                KnnUtils.resolveKnnQueryAndBoost(
                    knnQuery, 1.0f, indexSearcher, VectorDiagnostics.newBuilder()));
    assertEquals(
        "Vector total hits not available after rewrite of knn query: NullTotalHitsVectorQuery",
        e.getMessage());
  }
}
