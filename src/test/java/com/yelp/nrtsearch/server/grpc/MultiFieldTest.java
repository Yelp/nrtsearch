/*
 * Copyright 2020 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiFieldTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerMultiFields.json");
  }

  public static final List<String> ALL_FIELDS =
      Arrays.asList(
          "doc_id",
          "field_1",
          "field_1.keyword",
          "field_2",
          "field_2.number",
          "field_2.number.keyword",
          "field_3",
          "field_3.keyword",
          "field_3.text");

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("field_1", MultiValuedField.newBuilder().addValue("First Field").build())
            .putFields("field_2", MultiValuedField.newBuilder().addValue("10").build())
            .putFields("field_3", MultiValuedField.newBuilder().addValue("50").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("field_1", MultiValuedField.newBuilder().addValue("Second Field").build())
            .putFields("field_2", MultiValuedField.newBuilder().addValue("20").build())
            .putFields("field_3", MultiValuedField.newBuilder().addValue("100").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testRetrieveChildField() {
    SearchResponse response = doQuery(Query.newBuilder().build());
    assertFields(response, "1", "2");
  }

  @Test
  public void testQueryChildField() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("field_1").setQuery("First").build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("field_1").setQuery("Second").build())
                .build());
    assertFields(response, "2");

    response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("field_1").setQuery("Field").build())
                .build());
    assertFields(response, "1", "2");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("field_1.keyword")
                        .setTextValue("First Field")
                        .build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("field_1.keyword")
                        .setTextValue("Second Field")
                        .build())
                .build());
    assertFields(response, "2");
  }

  @Test
  public void testQueryNestedChildField() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(MatchQuery.newBuilder().setField("field_2").setQuery("10").build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(MatchQuery.newBuilder().setField("field_2").setQuery("20").build())
                .build());
    assertFields(response, "2");

    response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("field_2.number")
                        .setLower("5")
                        .setUpper("15")
                        .build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("field_2.number")
                        .setLower("15")
                        .setUpper("25")
                        .build())
                .build());
    assertFields(response, "2");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("field_2.number.keyword")
                        .setTextValue("10")
                        .build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("field_2.number.keyword")
                        .setTextValue("20")
                        .build())
                .build());
    assertFields(response, "2");
  }

  @Test
  public void testQueryChildFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("field_3")
                        .setLower("0")
                        .setUpper("75")
                        .build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setRangeQuery(
                    RangeQuery.newBuilder()
                        .setField("field_3")
                        .setLower("75")
                        .setUpper("125")
                        .build())
                .build());
    assertFields(response, "2");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder().setField("field_3.keyword").setTextValue("50").build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder().setField("field_3.keyword").setTextValue("100").build())
                .build());
    assertFields(response, "2");

    response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("field_3.text").setQuery("50").build())
                .build());
    assertFields(response, "1");

    response =
        doQuery(
            Query.newBuilder()
                .setMatchQuery(
                    MatchQuery.newBuilder().setField("field_3.text").setQuery("100").build())
                .build());
    assertFields(response, "2");
  }

  private SearchResponse doQuery(Query query) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(ALL_FIELDS)
                .setQuery(query)
                .build());
  }

  private void assertFields(SearchResponse response, String... expectedIds) {
    Set<String> seenSet = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      seenSet.add(id);
      if (id.equals("1")) {
        assertEquals(
            "First Field", hit.getFieldsOrThrow("field_1").getFieldValue(0).getTextValue());
        assertEquals(
            "First Field", hit.getFieldsOrThrow("field_1.keyword").getFieldValue(0).getTextValue());
        assertEquals("10", hit.getFieldsOrThrow("field_2").getFieldValue(0).getTextValue());
        assertEquals(10, hit.getFieldsOrThrow("field_2.number").getFieldValue(0).getIntValue());
        assertEquals(
            "10", hit.getFieldsOrThrow("field_2.number.keyword").getFieldValue(0).getTextValue());
        assertEquals(50, hit.getFieldsOrThrow("field_3").getFieldValue(0).getIntValue());
        assertEquals("50", hit.getFieldsOrThrow("field_3.keyword").getFieldValue(0).getTextValue());
        assertEquals("50", hit.getFieldsOrThrow("field_3.text").getFieldValue(0).getTextValue());
      } else if (id.equals("2")) {
        assertEquals(
            "Second Field", hit.getFieldsOrThrow("field_1").getFieldValue(0).getTextValue());
        assertEquals(
            "Second Field",
            hit.getFieldsOrThrow("field_1.keyword").getFieldValue(0).getTextValue());
        assertEquals("20", hit.getFieldsOrThrow("field_2").getFieldValue(0).getTextValue());
        assertEquals(20, hit.getFieldsOrThrow("field_2.number").getFieldValue(0).getIntValue());
        assertEquals(
            "20", hit.getFieldsOrThrow("field_2.number.keyword").getFieldValue(0).getTextValue());
        assertEquals(100, hit.getFieldsOrThrow("field_3").getFieldValue(0).getIntValue());
        assertEquals(
            "100", hit.getFieldsOrThrow("field_3.keyword").getFieldValue(0).getTextValue());
        assertEquals("100", hit.getFieldsOrThrow("field_3.text").getFieldValue(0).getTextValue());
      } else {
        fail("Unknown id: " + id);
      }
    }
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedIds));
    assertEquals(expectedSet, seenSet);
  }
}
