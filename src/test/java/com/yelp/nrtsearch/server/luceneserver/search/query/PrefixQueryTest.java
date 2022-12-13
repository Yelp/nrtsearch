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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.junit.ClassRule;
import org.junit.Test;

public class PrefixQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsPrefixQuery.json");
  }

  protected void initIndex(String name) throws Exception {
    List<String> textValues =
        List.of(
            "prefix1a",
            "prefix1b",
            "prefix1c",
            "prefix2a",
            "prefix2b",
            "prefix2c",
            "prefix2d",
            "not_prefix1",
            "not_prefix2");
    List<AddDocumentRequest> docs = new ArrayList<>();
    int index = 0;
    for (String textValue : textValues) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(Integer.toString(index)).build())
              .putFields("text_field", MultiValuedField.newBuilder().addValue(textValue).build())
              .build();
      docs.add(request);
      index++;
    }
    addDocuments(docs.stream());
  }

  @Test
  public void testTextPrefixQuery() {
    SearchResponse searchResponse = doQuery("text_field", "prefix1");
    assertIds(searchResponse, 0, 1, 2);
    searchResponse = doQuery("text_field", "prefix2");
    assertIds(searchResponse, 3, 4, 5, 6);
    searchResponse = doQuery("text_field", "prefix");
    assertIds(searchResponse, 0, 1, 2, 3, 4, 5, 6);
    searchResponse = doQuery("text_field", "other");
    assertIds(searchResponse);
  }

  @Test
  public void testAtomPrefixQuery() {
    SearchResponse searchResponse = doQuery("text_field.atom", "prefix1");
    assertIds(searchResponse, 0, 1, 2);
    searchResponse = doQuery("text_field.atom", "prefix2");
    assertIds(searchResponse, 3, 4, 5, 6);
    searchResponse = doQuery("text_field.atom", "prefix");
    assertIds(searchResponse, 0, 1, 2, 3, 4, 5, 6);
    searchResponse = doQuery("text_field.atom", "other");
    assertIds(searchResponse);
  }

  @Test
  public void testFieldNotExist() {
    try {
      doQuery("invalid", "prefix");
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains("field \"invalid\" is unknown: it was not registered with registerField"));
    }
  }

  @Test
  public void testFieldNotIndexable() {
    try {
      doQuery("virtual", "prefix");
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field \"prefix\" is not indexable"));
    }
  }

  @Test
  public void testFieldNoTerms() {
    try {
      doQuery("text_field.not_searchable", "prefix");
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage().contains("Field \"text_field.not_searchable\" is not indexed with terms"));
    }
  }

  @Test
  public void testSetsRewrite_constantScore() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(
            com.yelp.nrtsearch.server.grpc.RewriteMethod.CONSTANT_SCORE, 0);
    assertSame(MultiTermQuery.CONSTANT_SCORE_REWRITE, rewriteMethod);
  }

  @Test
  public void testSetsRewrite_constantScoreBoolean() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(
            com.yelp.nrtsearch.server.grpc.RewriteMethod.CONSTANT_SCORE_BOOLEAN, 0);
    assertSame(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE, rewriteMethod);
  }

  @Test
  public void testSetsRewrite_scoringBoolean() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(
            com.yelp.nrtsearch.server.grpc.RewriteMethod.SCORING_BOOLEAN, 0);
    assertSame(MultiTermQuery.SCORING_BOOLEAN_REWRITE, rewriteMethod);
  }

  @Test
  public void testSetsRewrite_topTermsBlendedFreqs() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(
            com.yelp.nrtsearch.server.grpc.RewriteMethod.TOP_TERMS_BLENDED_FREQS, 5);
    assertTrue(rewriteMethod instanceof MultiTermQuery.TopTermsBlendedFreqScoringRewrite);
    assertEquals(5, ((MultiTermQuery.TopTermsBlendedFreqScoringRewrite) rewriteMethod).getSize());
  }

  @Test
  public void testSetsRewrite_topTermsBoost() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(
            com.yelp.nrtsearch.server.grpc.RewriteMethod.TOP_TERMS_BOOST, 6);
    assertTrue(rewriteMethod instanceof MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite);
    assertEquals(
        6, ((MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite) rewriteMethod).getSize());
  }

  @Test
  public void testSetsRewrite_topTerms() throws IOException {
    RewriteMethod rewriteMethod =
        getRewriteMethodOfBuiltQuery(com.yelp.nrtsearch.server.grpc.RewriteMethod.TOP_TERMS, 7);
    assertTrue(rewriteMethod instanceof MultiTermQuery.TopTermsScoringBooleanQueryRewrite);
    assertEquals(7, ((MultiTermQuery.TopTermsScoringBooleanQueryRewrite) rewriteMethod).getSize());
  }

  private RewriteMethod getRewriteMethodOfBuiltQuery(
      com.yelp.nrtsearch.server.grpc.RewriteMethod rewriteMethodGrpc, int topTermsSize)
      throws IOException {
    IndexState state = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    org.apache.lucene.search.Query query =
        QueryNodeMapper.getInstance()
            .getQuery(
                Query.newBuilder()
                    .setPrefixQuery(
                        PrefixQuery.newBuilder()
                            .setField("text_field")
                            .setPrefix("prefix")
                            .setRewrite(rewriteMethodGrpc)
                            .setRewriteTopTermsSize(topTermsSize)
                            .build())
                    .build(),
                state);
    assertTrue(query instanceof org.apache.lucene.search.PrefixQuery);
    return ((org.apache.lucene.search.PrefixQuery) query).getRewriteMethod();
  }

  private SearchResponse doQuery(String field, String text) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(20)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setPrefixQuery(
                            PrefixQuery.newBuilder().setField(field).setPrefix(text).build())
                        .build())
                .build());
  }

  private void assertIds(SearchResponse response, int... ids) {
    Set<Integer> uniqueIds = new HashSet<>();
    for (int id : ids) {
      uniqueIds.add(id);
    }
    assertEquals(uniqueIds.size(), response.getHitsCount());

    Set<Integer> responseIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      responseIds.add(
          Integer.parseInt(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
    assertEquals(uniqueIds, responseIds);
  }
}
