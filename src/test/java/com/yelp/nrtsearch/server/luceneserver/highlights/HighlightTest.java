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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.Highlight.Settings;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class HighlightTest extends ServerTestCase {

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
                MultiValuedField.newBuilder().addValue("the food here is amazing").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "comment",
                MultiValuedField.newBuilder().addValue("the food here is pretty good").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testHighlight() {
    SearchResponse response = doHighlightQuery(Settings.newBuilder().build());

    assertFields(response, "1", "2");

    assertThat(response.getHits(0).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo("the <em>food</em> here is amazing");
    assertThat(response.getHits(1).getHighlightsMap().get("comment").getFragments(0))
        .isEqualTo("the <em>food</em> here is pretty good");
  }

  private String indexName() {
    return getIndices().get(0);
  }

  private SearchResponse doHighlightQuery(Settings settings) {
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
                .setHighlight(Highlight.newBuilder().putFields("comment", settings))
                .build());
  }

  private void assertFields(SearchResponse response, String... expectedIds) {
    Set<String> seenSet = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      seenSet.add(id);
      if (id.equals("1")) {
        assertEquals(
            "the food here is amazing",
            hit.getFieldsOrThrow("comment").getFieldValue(0).getTextValue());
      } else if (id.equals("2")) {
        assertEquals(
            "the food here is pretty good",
            hit.getFieldsOrThrow("comment").getFieldValue(0).getTextValue());
      } else {
        fail("Unknown id: " + id);
      }
    }
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedIds));
    assertEquals(expectedSet, seenSet);
  }
}