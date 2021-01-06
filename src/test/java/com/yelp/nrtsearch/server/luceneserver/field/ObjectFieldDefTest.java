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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class ObjectFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsObject.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    Gson gson = new Gson();
    Map<String, Object> map = new HashMap<>();
    map.put("hours", List.of(1));
    map.put("zipcode", List.of("94105"));
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("partner_id", "abcd");
    innerMap.put("partner_name", "efg");
    map.put("partner", innerMap);

    Map<String, Object> pickup1 = new HashMap<>();
    pickup1.put("name", "AAA");
    pickup1.put("hours", List.of(2));

    Map<String, Object> pickup2 = new HashMap<>();
    pickup2.put("name", "BBB");
    pickup2.put("hours", List.of(3));

    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "delivery_areas",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue(gson.toJson(map)).build())
            .putFields(
                "pickup_partners",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue(gson.toJson(pickup1))
                    .addValue(gson.toJson(pickup2))
                    .build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testObjectChildFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("delivery_areas.zipcode")
                        .setTextValue("94105")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  @Test
  public void testFlattenedNestedObjectChildFields() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setTermQuery(
                    TermQuery.newBuilder()
                        .setField("delivery_areas.partner.partner_name")
                        .setTextValue("efg")
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  @Test
  public void testSimpleNestedDocs() {
    SearchResponse response =
        doQuery(
            Query.newBuilder()
                .setNestedQuery(
                    NestedQuery.newBuilder()
                        .setPath("pickup_partners")
                        .setQUERY(
                            Query.newBuilder()
                                .setTermQuery(
                                    TermQuery.newBuilder()
                                        .setField("pickup_partners.name")
                                        .setTextValue("AAA")
                                        .build()))
                        .build())
                .build(),
            List.of("doc_id"));
    assertFields(response, "1");
  }

  private SearchResponse doQuery(Query query, List<String> fields) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(fields)
                .setQuery(query)
                .build());
  }

  private void assertFields(SearchResponse response, String... expectedIds) {
    Set<String> seenSet = new HashSet<>();
    for (SearchResponse.Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      seenSet.add(id);
    }
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedIds));
    assertEquals(seenSet, expectedSet);
  }
}
