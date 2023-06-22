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
package com.yelp.nrtsearch.server.luceneserver.highlights;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.grpc.Highlight.Type;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.PhraseQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class NRTFastVectorHighlighterTest extends ServerTestCase {

  private static final List<String> ALL_FIELDS = List.of("doc_id", "comment");

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/highlights/register_fields_highlights.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "comment",
                MultiValuedField.newBuilder()
                    .addValue("the food here is amazing, service was good")
                    .build())
            .putFields(
                "comment2",
                MultiValuedField.newBuilder()
                    .addValue("This is my regular place, the food is good")
                    .build())
            .putFields(
                "comment_multivalue",
                MultiValuedField.newBuilder()
                    .addAllValue(
                        List.of(
                            "The food is good there, but the service is terrible.",
                            "I personally don't like the staff at this place",
                            "Not all food are good."))
                    .build())
            .putFields(
                "boundary_scanner_field",
                MultiValuedField.newBuilder()
                    .addValue(
                        "This is a super longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle-it-in a very decent way and  stops at.")
                    .build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "comment",
                MultiValuedField.newBuilder()
                    .addValue(
                        "This is my first time eating at this restaurant. The food here is pretty good, the service could be better. My favorite food was chilly chicken.")
                    .build())
            .putFields(
                "comment2",
                MultiValuedField.newBuilder()
                    .addValue("There is some amazing food and also drinks here. Must visit!")
                    .build())
            .putFields(
                "comment_multivalue",
                MultiValuedField.newBuilder()
                    .addValue("High quality food. Fresh and delicious!")
                    .build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testBasicHighlight() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(Settings.newBuilder().setScoreOrdered(BoolValue.of(true)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo("the <em>food</em> here is amazing, service was good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo(
            "restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithName() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(Settings.newBuilder().setHighlighterType(Type.FAST_VECTOR))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo("the <em>food</em> here is amazing, service was good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo(
            "restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithUnknownName() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(
                Settings.newBuilder()
                    .setHighlighterType(Type.CUSTOM)
                    .setCustomHighlighterName("doesn't-exist"))
            .build();
    assertThatThrownBy(() -> doHighlightQuery(highlight))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Unknown highlighter name");
  }

  @Test
  public void testHighlightMultivalueField() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly(
            "The <em>food</em> is good there, but the service is terrible.",
            "Not all <em>food</em> are good.");
    assertThat(response.getHits(1).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("High quality <em>food</em>. Fresh and delicious!");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightGlobalSettings() {
    Settings settings =
        Settings.newBuilder()
            .addPreTags("<START>")
            .addPostTags("<END>")
            .setFragmentSize(UInt32Value.newBuilder().setValue(18))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(3))
            .setHighlightQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("comment").setQuery("food is good")))
            .setScoreOrdered(BoolValue.of(true))
            .build();
    Highlight highlight = Highlight.newBuilder().setSettings(settings).addFields("comment").build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "the <START>food<END> here <START>is<END> amazing", "service was <START>good<END>");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "The <START>food<END> here <START>is<END> pretty",
            "This <START>is<END> my first time",
            "pretty <START>good<END>, the service");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightFieldSettings() {
    Settings globalSettings =
        Settings.newBuilder()
            .addPreTags("<s>")
            .addPostTags("<e>")
            .setFragmentSize(UInt32Value.newBuilder().setValue(50))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(5))
            .setHighlightQuery(
                Query.newBuilder()
                    .setMatchQuery(MatchQuery.newBuilder().setField("comment").setQuery("pretty")))
            .setScoreOrdered(BoolValue.of(true))
            .build();
    Settings fieldSettings =
        Settings.newBuilder()
            .addPreTags("<START>")
            .addPostTags("<END>")
            .setFragmentSize(UInt32Value.newBuilder().setValue(18))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(3))
            .setHighlightQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("comment").setQuery("food is good")))
            .build();
    Highlight highlight =
        Highlight.newBuilder()
            .setSettings(globalSettings)
            .addFields("comment")
            .putFieldSettings("comment", fieldSettings)
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "the <START>food<END> here <START>is<END> amazing", "service was <START>good<END>");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "The <START>food<END> here <START>is<END> pretty",
            "This <START>is<END> my first time",
            "pretty <START>good<END>, the service");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testNotScoreOrdered() {
    Settings globalSettings =
        Settings.newBuilder()
            .setHighlightQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("comment").setQuery("food is good")))
            .setFragmentSize(UInt32Value.newBuilder().setValue(18))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(3))
            .setScoreOrdered(BoolValue.of(false))
            .build();
    Highlight highlight =
        Highlight.newBuilder().setSettings(globalSettings).addFields("comment").build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly("the <em>food</em> here <em>is</em> amazing", "service was <em>good</em>");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "This <em>is</em> my first time",
            "The <em>food</em> here <em>is</em> pretty",
            "pretty <em>good</em>, the service");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightsAbsentForOneHit() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setField("comment")
                                    .setSlop(5)
                                    .addTerms("food")
                                    .addTerms("pretty")
                                    .addTerms("good"))))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap()).isEmpty();
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "first time eating at this restaurant. The <em>food</em> here is <em>pretty good</em>, the service could be better. My favorite");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightIncompatibleField() {
    Highlight highlight = Highlight.newBuilder().addFields("comment.no_search").build();
    try {
      doHighlightQuery(highlight);
      fail("No error for invalid field");
    } catch (StatusRuntimeException e) {
      assertThat(e.getMessage())
          .contains("Field comment.no_search is not searchable and cannot support highlights");
    }

    highlight = Highlight.newBuilder().addFields("comment.no_store").build();
    try {
      doHighlightQuery(highlight);
      fail("No error for invalid field");
    } catch (StatusRuntimeException e) {
      assertThat(e.getMessage())
          .contains(
              "Field comment.no_store is not stored and cannot support fast-vector-highlighter");
    }

    highlight = Highlight.newBuilder().addFields("comment.no_term_vectors_with_offsets").build();
    try {
      doHighlightQuery(highlight);
      fail("No error for invalid field");
    } catch (StatusRuntimeException e) {
      assertThat(e.getMessage())
          .contains(
              "Field comment.no_term_vectors_with_offsets does not have term vectors with positions and offsets and cannot support fast-vector-highlighter");
    }
  }

  @Test
  public void testHighlightMaxNumFragmentsZeroGlobal() {
    Settings globalSettings =
        Settings.newBuilder()
            .setFragmentSize(UInt32Value.newBuilder().setValue(10))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(0))
            .build();
    Highlight highlight =
        Highlight.newBuilder().setSettings(globalSettings).addFields("comment").build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly("the <em>food</em> here is amazing, service was good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "This is my first time eating at this restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken.");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightMaxNumFragmentsZeroForField() {
    Settings globalSettings =
        Settings.newBuilder()
            .setFragmentSize(UInt32Value.newBuilder().setValue(10))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(1))
            .build();
    Settings fieldSettings =
        Settings.newBuilder()
            .setFragmentSize(UInt32Value.newBuilder().setValue(10))
            .setMaxNumberOfFragments(UInt32Value.newBuilder().setValue(0))
            .build();
    Highlight highlight =
        Highlight.newBuilder()
            .setSettings(globalSettings)
            .addFields("comment")
            .putFieldSettings("comment", fieldSettings)
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly("the <em>food</em> here is amazing, service was good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragmentsList())
        .containsExactly(
            "This is my first time eating at this restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken.");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightFieldMatchFalse() {
    Highlight highlight =
        Highlight.newBuilder()
            .setSettings(Settings.newBuilder().setFieldMatch(BoolValue.of(false)))
            .addFields("comment2")
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHitsCount()).isEqualTo(2);
    assertThat(response.getHits(0).getHighlightsMap().get("comment2").getFragmentsList())
        .containsExactly("This is my regular place, the <em>food</em> is good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment2").getFragmentsList())
        .containsExactly("There is some amazing <em>food</em> and also drinks here. Must visit!");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testHighlightFieldMatchTrue() {
    Highlight highlight =
        Highlight.newBuilder()
            .setSettings(
                Settings.newBuilder()
                    .setFieldMatch(BoolValue.of(true))
                    .setScoreOrdered(BoolValue.of(true)))
            .addFields("comment2")
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHitsCount()).isEqualTo(2);
    assertThat(response.getHits(0).getHighlightsMap()).isEmpty();
    assertThat(response.getHits(1).getHighlightsMap()).isEmpty();
  }

  @Test
  public void testMaxFragmentSize() {
    Highlight highlight =
        Highlight.newBuilder()
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("comment_multivalue")
                                    .setQuery("food")))
                    .setMaxNumberOfFragments(UInt32Value.of(2))
                    .setFragmentSize(UInt32Value.of(0))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setFieldMatch(BoolValue.of(true)))
            .addFields("comment_multivalue")
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHitsCount()).isEqualTo(2);
    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly(
            "The <em>food</em> is good there, but the service is terrible.",
            "Not all <em>food</em> are good.");
    assertThat(response.getHits(1).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("High quality <em>food</em>. Fresh and delicious!");
  }

  @Test
  public void testBasicHighlightWithExplicitBoundaryScanner() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(
                Settings.newBuilder()
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("simple")))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo("the <em>food</em> here is amazing, service was good");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo(
            "restaurant. The <em>food</em> here is pretty good, the service could be better. My favorite <em>food</em> was chilly chicken");
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithWrongBoundaryScanner() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment")
            .setSettings(
                Settings.newBuilder()
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("doesnt_exist")))
            .build();

    assertThatThrownBy(() -> doHighlightQuery(highlight))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Unknown boundary scanner");
  }

  @Test
  public void testBasicHighlightWithCustomCharsBoundaryScanner() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("boundary_scanner_field")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("boundary_scanner_field")
                                    .setTextValue("super")
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("simple"))
                    .setFragmentSize(UInt32Value.of(75))
                    .setBoundaryChars(StringValue.of("-")))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("boundary_scanner_field").getFragments(0))
        .isEqualTo(
            "This is a <em>super</em> longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle");
    assertThat(response.getHits(1).getHighlightsCount()).isEqualTo(0);
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithBoundaryScannerAndMaxScan() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("boundary_scanner_field")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("boundary_scanner_field")
                                    .setTextValue("super")
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("simple"))
                    .setFragmentSize(UInt32Value.of(75))
                    .setBoundaryMaxScan(UInt32Value.of(100)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("boundary_scanner_field").getFragments(0))
        .isEqualTo(
            "This is a <em>super</em> longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle-it-in");
    assertThat(response.getHits(1).getHighlightsCount()).isEqualTo(0);
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithWordBoundaryScanner() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("boundary_scanner_field")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("boundary_scanner_field")
                                    .setTextValue("super")
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("word"))
                    .setFragmentSize(UInt32Value.of(75)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("boundary_scanner_field").getFragments(0))
        .isEqualTo(
            "This is a <em>super</em> longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle-it-in");
    assertThat(response.getHits(1).getHighlightsCount()).isEqualTo(0);
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithSentenceBoundaryScanner() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("boundary_scanner_field")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("boundary_scanner_field")
                                    .setTextValue("super")
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("sentence"))
                    .setFragmentSize(UInt32Value.of(75)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("boundary_scanner_field").getFragments(0))
        .isEqualTo(
            "This is a <em>super</em> longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle-it-in a very decent way and  stops at. ");
    assertThat(response.getHits(1).getHighlightsCount()).isEqualTo(0);
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  @Test
  public void testBasicHighlightWithSentenceBoundaryScannerAndExplicitLocale() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("boundary_scanner_field")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("boundary_scanner_field")
                                    .setTextValue("super")
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setBoundaryScanner(StringValue.of("sentence"))
                    .setBoundaryScannerLocale(StringValue.of("en-US"))
                    .setFragmentSize(UInt32Value.of(75)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertFields(response);

    assertThat(response.getHits(0).getHighlightsMap().get("boundary_scanner_field").getFragments(0))
        .isEqualTo(
            "This is a <em>super</em> longWordICouldEverImagineAndTheBoundaryScannerShouldProperlyHandle-it-in a very decent way and  stops at. ");
    assertThat(response.getHits(1).getHighlightsCount()).isEqualTo(0);
    assertThat(response.getDiagnostics().getHighlightTimeMs()).isGreaterThan(0);
  }

  private String indexName() {
    return getIndices().get(0);
  }

  private SearchResponse doHighlightQuery(Highlight highlight) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(indexName())
                .setStartHit(0)
                .setTopHits(2)
                .addAllRetrieveFields(ALL_FIELDS)
                .setQuery(
                    Query.newBuilder()
                        .setMatchQuery(
                            MatchQuery.newBuilder().setField("comment").setQuery("food")))
                .setHighlight(highlight)
                .build());
  }

  private void assertFields(SearchResponse response) {
    Set<String> seenSet = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      seenSet.add(id);
      if (id.equals("1")) {
        assertThat(hit.getFieldsOrThrow("comment").getFieldValue(0).getTextValue())
            .isEqualTo("the food here is amazing, service was good");
      } else if (id.equals("2")) {
        assertThat(hit.getFieldsOrThrow("comment").getFieldValue(0).getTextValue())
            .isEqualTo(
                "This is my first time eating at this restaurant. The food here is pretty good, the service could be better. My favorite food was chilly chicken.");
      } else {
        fail("Unknown id: " + id);
      }
    }
    assertThat(seenSet).containsExactly("1", "2");
  }
}
