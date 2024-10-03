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
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Test;

public class ExistsQueryTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected Gson gson = new GsonBuilder().serializeNulls().create();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsExistsQuery.json");
  }

  protected void initIndex(String name) throws Exception {
    // No exists matches
    AddDocumentRequest emptyBusiness =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", MultiValuedField.newBuilder().addValue("0").build())
            .build();

    // Empty string on name should match exists query
    AddDocumentRequest minimalBusiness =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("name", MultiValuedField.newBuilder().addValue("").build())
            .putFields("pickup_partners", MultiValuedField.newBuilder().addValue("{}").build())
            .putFields("delivery_areas", MultiValuedField.newBuilder().addValue("{}").build())
            .build();

    // Should match all exists queries
    Map<String, Object> pickup2 = Map.of("hours", List.of(2));
    Map<String, Object> deliveryAreas2 = Map.of("partner", Map.of("partner_id", 123));

    AddDocumentRequest fullBusiness =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("name", MultiValuedField.newBuilder().addValue("restaurantA").build())
            .putFields(
                "pickup_partners",
                MultiValuedField.newBuilder().addValue(gson.toJson(pickup2)).build())
            .putFields(
                "delivery_areas",
                MultiValuedField.newBuilder().addValue(gson.toJson(deliveryAreas2)).build())
            .build();

    addDocuments(List.of(emptyBusiness, minimalBusiness, fullBusiness).stream());
  }

  @Test(expected = NullPointerException.class)
  public void testAddNullMultiValuedField() {
    // Verify that clientlib doesn't allow null
    AddDocumentRequest.newBuilder()
        .setIndexName("test_index")
        .putFields("id", MultiValuedField.newBuilder().addValue("0").build())
        .putFields("name", MultiValuedField.newBuilder().addValue(null).build())
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testAddNullField() {
    // Verify that clientlib doesn't allow null
    AddDocumentRequest.newBuilder()
        .setIndexName("test_index")
        .putFields("id", MultiValuedField.newBuilder().addValue("0").build())
        .putFields("name", null)
        .build();
  }

  @Test
  public void testTopLevelField() {
    // Empty strings become valid lucene fields
    ExistsQuery existsQuery = ExistsQuery.newBuilder().setField("name").build();
    queryAndVerifyIds(existsQuery, "1", "2");
  }

  // TODO:(samir|2020-01-21): Test exists query on nested object itself (Ex. pickup_partners)
  @Test
  public void testNestedField() {
    Query nestedQuery =
        Query.newBuilder()
            .setNestedQuery(
                NestedQuery.newBuilder()
                    .setPath("pickup_partners")
                    .setQuery(
                        Query.newBuilder()
                            .setExistsQuery(
                                ExistsQuery.newBuilder().setField("pickup_partners.hours").build()))
                    .build())
            .build();

    queryAndVerifyIds(nestedQuery, "2");
  }

  // TODO:(samir|2020-01-21): Test exists query on object itself
  //  (Ex. delivery_areas or delivery_areas.partner)
  @Test
  public void testInnerObject() {
    ExistsQuery existsQuery =
        ExistsQuery.newBuilder().setField("delivery_areas.partner.partner_id").build();
    queryAndVerifyIds(existsQuery, "2");
  }

  private SearchResponse executeQuery(Query query, String retrieveFields) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(query)
                .addRetrieveFields(retrieveFields)
                .build());
  }

  private void queryAndVerifyIds(ExistsQuery existsQuery, String... expectedIds) {
    Query query = Query.newBuilder().setExistsQuery(existsQuery).build();
    queryAndVerifyIds(query, expectedIds);
  }

  private void queryAndVerifyIds(Query query, String... expectedIds) {
    SearchResponse response = executeQuery(query, "id");
    List<String> idList = Arrays.asList(expectedIds);
    assertEquals(idList.size(), response.getHitsCount());
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertTrue(idList.contains(hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue()));
    }
  }
}
