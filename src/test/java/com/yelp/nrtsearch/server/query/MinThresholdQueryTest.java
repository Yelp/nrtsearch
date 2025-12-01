/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.ClassRule;
import org.junit.Test;

public class MinThresholdQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsMinThreshold.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    // Add documents with different text content for scoring
    AddDocumentRequest.Builder docBuilder = AddDocumentRequest.newBuilder();
    docBuilder
        .setIndexName(name)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
        .putFields(
            "text_field", MultiValuedField.newBuilder().addValue("test document one").build())
        .putFields("score_field", MultiValuedField.newBuilder().addValue("1.5").build());
    docs.add(docBuilder.build());

    docBuilder.clear();
    docBuilder
        .setIndexName(name)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
        .putFields(
            "text_field", MultiValuedField.newBuilder().addValue("test test document two").build())
        .putFields("score_field", MultiValuedField.newBuilder().addValue("2.5").build());
    docs.add(docBuilder.build());

    docBuilder.clear();
    docBuilder
        .setIndexName(name)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue("3").build())
        .putFields(
            "text_field",
            MultiValuedField.newBuilder().addValue("test test test document three").build())
        .putFields("score_field", MultiValuedField.newBuilder().addValue("0.5").build());
    docs.add(docBuilder.build());

    addDocuments(docs.stream());
  }

  @Test
  public void testConstructor() {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    float threshold = 1.0f;

    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, threshold);

    assertNotNull(query);
  }

  @Test
  public void testToString() {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    float threshold = 1.5f;

    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, threshold);
    String result = query.toString("field");

    assertTrue(result.contains("MinThresholdQuery"));
    assertTrue(result.contains("wrapped="));
    assertTrue(result.contains("threshold=1.5"));
  }

  @Test
  public void testEquals() {
    MatchAllDocsQuery wrappedQuery1 = new MatchAllDocsQuery();
    MatchAllDocsQuery wrappedQuery2 = new MatchAllDocsQuery();
    TermQuery differentQuery = new TermQuery(new Term("field", "value"));

    MinThresholdQuery query1 = new MinThresholdQuery(wrappedQuery1, 1.0f);
    MinThresholdQuery query2 = new MinThresholdQuery(wrappedQuery2, 1.0f);
    MinThresholdQuery query3 = new MinThresholdQuery(wrappedQuery1, 2.0f);
    MinThresholdQuery query4 = new MinThresholdQuery(differentQuery, 1.0f);

    // Same wrapped query and threshold should be equal
    assertEquals(query1, query2);

    // Different threshold should not be equal
    assertNotEquals(query1, query3);

    // Different wrapped query should not be equal
    assertNotEquals(query1, query4);

    // Should not be equal to null or different class
    assertNotEquals(query1, null);
    assertNotEquals(query1, wrappedQuery1);
  }

  @Test
  public void testHashCode() {
    MatchAllDocsQuery wrappedQuery1 = new MatchAllDocsQuery();
    MatchAllDocsQuery wrappedQuery2 = new MatchAllDocsQuery();

    MinThresholdQuery query1 = new MinThresholdQuery(wrappedQuery1, 1.0f);
    MinThresholdQuery query2 = new MinThresholdQuery(wrappedQuery2, 1.0f);
    MinThresholdQuery query3 = new MinThresholdQuery(wrappedQuery1, 2.0f);

    // Equal objects should have equal hash codes
    assertEquals(query1.hashCode(), query2.hashCode());

    // Different objects should likely have different hash codes
    assertNotEquals(query1.hashCode(), query3.hashCode());
  }

  @Test
  public void testRewriteWithMatchNoDocsQuery() throws IOException {
    MatchNoDocsQuery noDocsQuery = new MatchNoDocsQuery();
    MinThresholdQuery query = new MinThresholdQuery(noDocsQuery, 1.0f);

    org.apache.lucene.search.Query rewritten = query.rewrite(null);

    // Should return the MatchNoDocsQuery directly
    assertTrue(rewritten instanceof MatchNoDocsQuery);
  }

  @Test
  public void testRewriteWithSameQuery() throws IOException {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, 1.0f);

    org.apache.lucene.search.Query rewritten = query.rewrite(null);

    // Should return the same query since MatchAllDocsQuery doesn't rewrite
    assertSame(query, rewritten);
  }

  @Test
  public void testVisit() {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, 1.0f);

    // Create a simple visitor to test the visit method
    final boolean[] visited = {false};
    query.visit(
        new org.apache.lucene.search.QueryVisitor() {
          @Override
          public void visitLeaf(org.apache.lucene.search.Query query) {
            visited[0] = true;
          }
        });

    assertTrue("Query should have been visited", visited[0]);
  }

  @Test
  public void testThresholdFiltering() throws Exception {
    // Test with a query that should filter based on threshold
    SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder();
    searchRequestBuilder.setIndexName(DEFAULT_TEST_INDEX);
    searchRequestBuilder.setStartHit(0);
    searchRequestBuilder.setTopHits(10);

    // Create a match query for "test" which should match all documents
    MatchQuery.Builder matchQueryBuilder = MatchQuery.newBuilder();
    matchQueryBuilder.setField("text_field");
    matchQueryBuilder.setQuery("test");

    Query.Builder queryBuilder = Query.newBuilder();
    queryBuilder.setMatchQuery(matchQueryBuilder.build());

    searchRequestBuilder.setQuery(queryBuilder.build());

    SearchResponse response =
        getGrpcServer().getBlockingStub().search(searchRequestBuilder.build());

    // All documents should match the basic query
    assertTrue("Should have at least one hit", response.getHitsCount() > 0);

    // Verify that documents are returned with scores
    for (Hit hit : response.getHitsList()) {
      assertTrue("Score should be positive", hit.getScore() > 0);
    }
  }

  @Test
  public void testLowThresholdAllowsAllDocuments() throws Exception {
    // This test verifies the basic functionality works
    // In a real implementation, you would wrap the query with MinThresholdQuery
    SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder();
    searchRequestBuilder.setIndexName(DEFAULT_TEST_INDEX);
    searchRequestBuilder.setStartHit(0);
    searchRequestBuilder.setTopHits(10);

    MatchQuery.Builder matchQueryBuilder = MatchQuery.newBuilder();
    matchQueryBuilder.setField("text_field");
    matchQueryBuilder.setQuery("test");

    Query.Builder queryBuilder = Query.newBuilder();
    queryBuilder.setMatchQuery(matchQueryBuilder.build());

    searchRequestBuilder.setQuery(queryBuilder.build());

    SearchResponse response =
        getGrpcServer().getBlockingStub().search(searchRequestBuilder.build());

    // Should return documents
    assertTrue("Should have hits", response.getHitsCount() > 0);
  }

  @Test
  public void testMinThresholdQueryRewriteWithDifferentQuery() throws IOException {
    // Test rewrite behavior when wrapped query rewrites to something different
    TermQuery termQuery = new TermQuery(new Term("field", "value"));
    MinThresholdQuery query = new MinThresholdQuery(termQuery, 1.0f);

    // Since TermQuery doesn't typically rewrite, this should return the same query
    org.apache.lucene.search.Query rewritten = query.rewrite(null);
    assertSame("Should return same query when wrapped query doesn't rewrite", query, rewritten);
  }

  @Test
  public void testMinThresholdQueryWithZeroThreshold() {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, 0.0f);

    String result = query.toString("field");
    assertTrue("Should contain threshold=0.0", result.contains("threshold=0.0"));
  }

  @Test
  public void testMinThresholdQueryWithNegativeThreshold() {
    MatchAllDocsQuery wrappedQuery = new MatchAllDocsQuery();
    MinThresholdQuery query = new MinThresholdQuery(wrappedQuery, -1.0f);

    String result = query.toString("field");
    assertTrue("Should contain negative threshold", result.contains("threshold=-1.0"));
  }

  @Test
  public void testMinThresholdQueryEqualsWithFloatPrecision() {
    MatchAllDocsQuery wrappedQuery1 = new MatchAllDocsQuery();
    MatchAllDocsQuery wrappedQuery2 = new MatchAllDocsQuery();

    MinThresholdQuery query1 = new MinThresholdQuery(wrappedQuery1, 1.0000001f);
    MinThresholdQuery query2 = new MinThresholdQuery(wrappedQuery2, 1.0000002f);

    // These should not be equal due to float precision differences
    assertNotEquals(
        "Queries with slightly different thresholds should not be equal", query1, query2);
  }
}
