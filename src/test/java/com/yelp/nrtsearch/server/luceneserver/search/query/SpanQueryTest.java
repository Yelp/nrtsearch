/*
 * Copyright 2023 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FuzzyQuery;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RegexpQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SpanMultiTermQueryWrapper;
import com.yelp.nrtsearch.server.grpc.SpanNearQuery;
import com.yelp.nrtsearch.server.grpc.SpanQuery;
import com.yelp.nrtsearch.server.grpc.Term;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.WildcardQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class SpanQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsSpanQuery.json");
  }

  protected void initIndex(String name) throws Exception {
    List<String> textValues =
        List.of(
            "The quick brown fox jumps over the lazy dog",
            "The quick brown fox jumps over the quick lazy dog",
            "The quick brown fox jumps over the lazy fox",
            "The text to test fuzzy search with potato",
            "The text to test fuzzy search with tomato ");
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

  private SearchRequest getSearchRequest(SpanQuery spanQuery) {
    return SearchRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .setTopHits(10)
        .addRetrieveFields("doc_id")
        .setQuery(Query.newBuilder().setSpanQuery(spanQuery).build())
        .build();
  }

  @Test
  public void testSpanNearQuery() {
    SpanNearQuery spanNearQuery =
        SpanNearQuery.newBuilder()
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("jumps").build())
                    .build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("dog").build())
                    .build())
            .setSlop(3)
            .setInOrder(true)
            .build();

    SpanQuery outerSpanQuery = SpanQuery.newBuilder().setSpanNearQuery(spanNearQuery).build();

    SearchResponse response =
        getGrpcServer().getBlockingStub().search(getSearchRequest(outerSpanQuery));

    assertIds(response, 0);
  }

  @Test
  public void testSpanNearQueryOrderFalse() {
    SpanNearQuery spanNearQuery =
        SpanNearQuery.newBuilder()
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("dog").build())
                    .build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("jumps").build())
                    .build())
            .setSlop(4)
            .setInOrder(false)
            .build();

    SpanQuery outerSpanQuery = SpanQuery.newBuilder().setSpanNearQuery(spanNearQuery).build();

    SearchResponse response =
        getGrpcServer().getBlockingStub().search(getSearchRequest(outerSpanQuery));

    assertIds(response, 0, 1);
  }

  @Test
  public void testSpanNearQueryOrderTrueNoResults() {
    SpanNearQuery spanNearQuery =
        SpanNearQuery.newBuilder()
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("jumps").build())
                    .build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("dog").build())
                    .build())
            .setSlop(2)
            .setInOrder(true)
            .build();
    SpanQuery outerSpanQuery = SpanQuery.newBuilder().setSpanNearQuery(spanNearQuery).build();

    SearchResponse response =
        getGrpcServer().getBlockingStub().search(getSearchRequest(outerSpanQuery));

    assertEquals(0, response.getHitsCount());
  }

  @Test
  public void testSpanNearQueryWithInlineSpanNearQuery() {
    SpanNearQuery innerSpanNearQuery =
        SpanNearQuery.newBuilder()
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("quick").build())
                    .build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("fox").build())
                    .build())
            .setSlop(1)
            .setInOrder(true)
            .build();

    SpanNearQuery spanNearQuery =
        SpanNearQuery.newBuilder()
            .addClauses(SpanQuery.newBuilder().setSpanNearQuery(innerSpanNearQuery).build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("jumps").build())
                    .build())
            .addClauses(
                SpanQuery.newBuilder()
                    .setSpanTermQuery(
                        TermQuery.newBuilder().setField("text_field").setTextValue("dog").build())
                    .build())
            .setSlop(3)
            .setInOrder(true)
            .build();

    SpanQuery spanQuery = SpanQuery.newBuilder().setSpanNearQuery(spanNearQuery).build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 0);
  }

  @Test
  public void testSpanMultiTermQueryWrapperWildcard() {

    // Create a Term object
    Term term = Term.newBuilder().setField("text_field").setText("do*").build();

    // Create a WildcardQuery object
    WildcardQuery wildcardQuery = WildcardQuery.newBuilder().setTerm(term).build();

    SpanMultiTermQueryWrapper spanMultiTermQueryWrapper =
        SpanMultiTermQueryWrapper.newBuilder().setWildcardQuery(wildcardQuery).build();

    SpanQuery spanQuery =
        SpanQuery.newBuilder().setSpanMultiTermQueryWrapper(spanMultiTermQueryWrapper).build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 0, 1);
  }

  @Test
  public void testSpanMultiTermQueryWrapperFuzzyQuery() {

    // Create a Term object that should match both potato and tomato
    Term term = Term.newBuilder().setField("text_field").setText("sotato").build();

    // Create a WildcardQuery object
    FuzzyQuery fuzzyQuery =
        FuzzyQuery.newBuilder()
            .setTerm(term)
            .setMaxEdits(org.apache.lucene.search.FuzzyQuery.defaultMaxEdits)
            .setPrefixLength(org.apache.lucene.search.FuzzyQuery.defaultPrefixLength)
            .setMaxExpansions(org.apache.lucene.search.FuzzyQuery.defaultMaxExpansions)
            .setTranspositions(org.apache.lucene.search.FuzzyQuery.defaultTranspositions)
            .build();

    SpanMultiTermQueryWrapper spanMultiTermQueryWrapper =
        SpanMultiTermQueryWrapper.newBuilder().setFuzzyQuery(fuzzyQuery).build();

    SpanQuery spanQuery =
        SpanQuery.newBuilder().setSpanMultiTermQueryWrapper(spanMultiTermQueryWrapper).build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 3, 4);
  }

  @Test
  public void testSpanMultiTermQueryWrapperFuzzyQueryMaxEdit() {

    // Create a Term object that should only match tomato
    Term term = Term.newBuilder().setField("text_field").setText("tomata").build();

    // Create a WildcardQuery object
    FuzzyQuery fuzzyQuery = FuzzyQuery.newBuilder().setTerm(term).setMaxEdits(1).build();

    SpanMultiTermQueryWrapper spanMultiTermQueryWrapper =
        SpanMultiTermQueryWrapper.newBuilder().setFuzzyQuery(fuzzyQuery).build();

    SpanQuery spanQuery =
        SpanQuery.newBuilder().setSpanMultiTermQueryWrapper(spanMultiTermQueryWrapper).build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 4);
  }

  @Test
  public void testSpanMultiTermQueryWrapperPrefixQuery() {

    // Create a Prefix Query object
    PrefixQuery prefixQuery =
        PrefixQuery.newBuilder()
            .setField("text_field")
            .setPrefix("qui")
            .setRewrite(com.yelp.nrtsearch.server.grpc.RewriteMethod.TOP_TERMS)
            .setRewriteTopTermsSize(7)
            .build();

    SpanMultiTermQueryWrapper spanMultiTermQueryWrapper =
        SpanMultiTermQueryWrapper.newBuilder().setPrefixQuery(prefixQuery).build();

    SpanQuery spanQuery =
        SpanQuery.newBuilder().setSpanMultiTermQueryWrapper(spanMultiTermQueryWrapper).build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 0, 1, 2);
  }

  @Test
  public void testSpanMultiTermQueryWrapperRegexpQuery() {

    Term term = Term.newBuilder().setField("text_field").setText("qu[a-z]+").build();
    // Create a RegexpQuery Query object
    RegexpQuery regexpQuery = RegexpQuery.newBuilder().setTerm(term).build();

    SpanMultiTermQueryWrapper spanMultiTermQueryWrapper =
        SpanMultiTermQueryWrapper.newBuilder().setRegexpQuery(regexpQuery).build();

    SpanQuery spanQuery =
        SpanQuery.newBuilder().setSpanMultiTermQueryWrapper(spanMultiTermQueryWrapper).build();

    SearchResponse response = getGrpcServer().getBlockingStub().search(getSearchRequest(spanQuery));

    assertIds(response, 0, 1, 2);
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
