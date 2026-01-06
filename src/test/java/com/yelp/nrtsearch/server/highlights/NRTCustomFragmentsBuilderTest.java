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
package com.yelp.nrtsearch.server.highlights;

import static org.assertj.core.api.Assertions.*;

import com.google.protobuf.BoolValue;
import com.google.protobuf.UInt32Value;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.grpc.PhraseQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class NRTCustomFragmentsBuilderTest extends ServerTestCase {

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
                "comment_multivalue",
                MultiValuedField.newBuilder()
                    .addAllValue(
                        List.of(
                            "apple apple apple apple apple apple apple apple apple apple apple apple apple",
                            "banana pear banana pear banana pear",
                            "gold foo diamond foo bar gold bar diamond"))
                    .build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testHighlightWithLimit() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("apple"))
                                                    .setBoost(2f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setMaxNumberOfHighlightedPhrasePerFragment(UInt32Value.of(8)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly(
            "<em>apple</em> <em>apple</em> <em>apple</em> <em>apple</em> <em>apple</em> <em>apple</em> <em>apple</em> <em>apple</em> apple apple apple apple apple");
  }

  @Test
  public void testHighlightWithTopBoostOnly() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("pear"))
                                                    .setBoost(2f)))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("banana"))
                                                    .setBoost(1f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setTopBoostOnly(BoolValue.of(true)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("banana <em>pear</em> banana <em>pear</em> banana <em>pear</em>");
  }

  @Test
  public void testHighlightWithTopBoostOnlyAndLimit() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("pear"))
                                                    .setBoost(2f)))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("banana"))
                                                    .setBoost(1f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setTopBoostOnly(BoolValue.of(true))
                    .setMaxNumberOfHighlightedPhrasePerFragment(UInt32Value.of(2)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("banana <em>pear</em> banana <em>pear</em> banana pear");
  }

  @Test
  public void testHighlightWithLimitOnTie() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("pear"))
                                                    .setBoost(2f)))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("banana"))
                                                    .setBoost(2f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setMaxNumberOfHighlightedPhrasePerFragment(UInt32Value.of(1)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("<em>banana</em> pear banana pear banana pear");
  }

  @Test
  public void testHighlightWithTopBoostOnlyOnTie() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("pear"))
                                                    .setBoost(2f)))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("banana"))
                                                    .setBoost(2f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setTopBoostOnly(BoolValue.of(true)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly(
            "<em>banana</em> <em>pear</em> <em>banana</em> <em>pear</em> <em>banana</em> <em>pear</em>");
  }

  @Test
  public void testHighlightWithTopBoostOnlyAndLimitOnTie() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("pear"))
                                                    .setBoost(2f)))
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .setTextValue("banana"))
                                                    .setBoost(2f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setTopBoostOnly(BoolValue.of(true))
                    .setMaxNumberOfHighlightedPhrasePerFragment(UInt32Value.of(2)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        .containsExactly("<em>banana</em> <em>pear</em> banana pear banana pear");
  }

  @Test
  public void testHighlightWithLimitOnPhraseWithSlop() {
    Highlight highlight =
        Highlight.newBuilder()
            .addFields("comment_multivalue")
            .setSettings(
                Settings.newBuilder()
                    .setHighlightQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setPhraseQuery(
                                                        PhraseQuery.newBuilder()
                                                            .setField("comment_multivalue")
                                                            .addAllTerms(List.of("gold", "diamond"))
                                                            .setSlop(1))
                                                    .setBoost(2f)))
                                    .build())
                            .build())
                    .setScoreOrdered(BoolValue.of(true))
                    .setDiscreteMultivalue(BoolValue.of(true))
                    .setMaxNumberOfHighlightedPhrasePerFragment(UInt32Value.of(1)))
            .build();
    SearchResponse response = doHighlightQuery(highlight);

    assertThat(response.getHits(0).getHighlightsMap().get("comment_multivalue").getFragmentsList())
        // "gold ? diamond" is treated as a single match, but not highlighted together due to fvh
        // internal design.
        // And the limit is applied to match count.
        .containsExactly("<em>gold</em> foo <em>diamond</em> foo bar gold bar diamond");
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
                .setTopHits(1)
                .setHighlight(highlight)
                .build());
  }
}
