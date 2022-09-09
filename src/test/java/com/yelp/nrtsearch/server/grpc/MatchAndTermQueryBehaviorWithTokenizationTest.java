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
package com.yelp.nrtsearch.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

public class MatchAndTermQueryBehaviorWithTokenizationTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String TEST_INDEX = "test_index";

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    // StandardAnalyzer will be used which filters StandardTokenizer with LowerCaseFilter
    return FieldDefRequest.newBuilder()
        .setIndexName(TEST_INDEX)
        .addField(
            Field.newBuilder().setName("doc_id").setType(FieldType.ATOM).setStoreDocValues(true))
        .addField(
            Field.newBuilder()
                .setName("tag")
                .setType(FieldType.TEXT)
                .setSearch(true)
                .setStoreDocValues(true)
                .addChildFields(
                    Field.newBuilder()
                        .setName("tokenized")
                        .setType(FieldType.TEXT)
                        .setSearch(true)
                        .setTokenize(true)
                        .setStoreDocValues(true))
                .addChildFields(
                    Field.newBuilder()
                        .setName("keyword_tokenized")
                        .setType(FieldType.TEXT)
                        .setAnalyzer(
                            Analyzer.newBuilder()
                                .setCustom(
                                    CustomAnalyzer.newBuilder()
                                        .addTokenFilters(
                                            NameAndParams.newBuilder().setName("lowercase"))
                                        .setTokenizer(
                                            NameAndParams.newBuilder().setName("keyword"))))
                        .setSearch(true)
                        .setTokenize(true)
                        .setStoreDocValues(true)))
        .build();
  }

  @Override
  protected void initIndex(String name) throws Exception {
    addDocuments(
        Stream.of(
            createAddDocumentRequest(1, "a"),
            createAddDocumentRequest(2, "A"),
            createAddDocumentRequest(3, "A A"),
            createAddDocumentRequest(4, "a a"),
            createAddDocumentRequest(5, "A a"),
            createAddDocumentRequest(6, "a A")));
  }

  private AddDocumentRequest createAddDocumentRequest(int docId, String tag) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(TEST_INDEX)
        .putFields(
            "doc_id",
            AddDocumentRequest.MultiValuedField.newBuilder()
                .addValue(String.valueOf(docId))
                .build())
        .putFields("tag", AddDocumentRequest.MultiValuedField.newBuilder().addValue(tag).build())
        .build();
  }

  @Test
  public void testMatchQuery_A() {
    SearchResponse response = doSearch(createMatchQuery("tag", "A"));

    // Match query on non-tokenized field does not return all possible matches after analysis
    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1");
  }

  @Test
  public void testMatchQuery_a() {
    SearchResponse response = doSearch(createMatchQuery("tag", "a"));

    // Match query on non-tokenized field only returns documents which exactly match the analyzed
    // query
    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1");
  }

  @Test
  public void testMatchQuery_tokenized_A() {
    SearchResponse response = doSearch(createMatchQuery("tag.tokenized", "A"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6");
  }

  @Test
  public void testMatchQuery_tokenized_a() {
    SearchResponse response = doSearch(createMatchQuery("tag.tokenized", "a"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6");
  }

  @Test
  public void testMatchQuery_keyword_tokenized_A() {
    SearchResponse response = doSearch(createMatchQuery("tag.keyword_tokenized", "A"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2");
  }

  @Test
  public void testMatchQuery_keyword_tokenized_a() {
    SearchResponse response = doSearch(createMatchQuery("tag.keyword_tokenized", "a"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2");
  }

  @Test
  public void testTermQuery_a() {
    SearchResponse response = doSearch(createTermQuery("tag", "a"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1");
  }

  @Test
  public void testTermQuery_A() {
    SearchResponse response = doSearch(createTermQuery("tag", "A"));

    assertThat(getDocIds(response)).containsExactlyInAnyOrder("2");
  }

  @Test
  public void testTermQuery_a_tokenized() {
    SearchResponse response = doSearch(createTermQuery("tag.tokenized", "a"));

    // Term query on tokenized field doesn't return exact match
    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6");
  }

  @Test
  public void testTermQuery_A_tokenized() {
    SearchResponse response = doSearch(createTermQuery("tag.tokenized", "A"));

    // Term query on tokenized field doesn't return exact match
    assertThat(getDocIds(response)).isEmpty();
  }

  @Test
  public void testTermQuery_a_keyword_tokenized() {
    SearchResponse response = doSearch(createTermQuery("tag.keyword_tokenized", "a"));

    // Term query on tokenized field doesn't return exact match
    assertThat(getDocIds(response)).containsExactlyInAnyOrder("1", "2");
  }

  @Test
  public void testTermQuery_A_keyword_tokenized() {
    SearchResponse response = doSearch(createTermQuery("tag.keyword_tokenized", "A"));

    // Term query on tokenized field doesn't return exact match
    assertThat(getDocIds(response)).isEmpty();
  }

  private Query createMatchQuery(String field, String tag) {
    return Query.newBuilder()
        .setMatchQuery(
            MatchQuery.newBuilder().setField(field).setQuery(tag).setMinimumNumberShouldMatch(1))
        .build();
  }

  private Query createTermQuery(String field, String tag) {
    return Query.newBuilder()
        .setTermQuery(TermQuery.newBuilder().setField(field).setTextValue(tag))
        .build();
  }

  private SearchResponse doSearch(Query query) {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(10)
            .addRetrieveFields("doc_id")
            .addRetrieveFields("tag")
            .setQuery(query)
            .build();
    return getGrpcServer().getBlockingStub().search(request);
  }

  private List<String> getDocIds(SearchResponse response) {
    return response.getHitsList().stream()
        .map(hit -> hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue())
        .collect(Collectors.toList());
  }
}
