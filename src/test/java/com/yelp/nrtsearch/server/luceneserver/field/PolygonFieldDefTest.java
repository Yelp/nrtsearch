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
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.protobuf.Struct;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Test;

public class PolygonFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsPolygon.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    Map<String, Object> doc = new HashMap<>();
    doc.put("type", "Polygon"); // Cannot be polygon...
    List<List<Double>> outer = new ArrayList<>();
    outer.add(List.of(100.0, 0.0));
    outer.add(List.of(101.0, 0.0));
    outer.add(List.of(101.0, 1.0));
    outer.add(List.of(100.0, 1.0));
    outer.add(List.of(100.0, 0.0));
    List<List<Double>> hole = new ArrayList<>();
    hole.add(List.of(100.2, 0.2));
    hole.add(List.of(100.8, 0.2));
    hole.add(List.of(100.8, 0.8));
    hole.add(List.of(100.2, 0.8));
    hole.add(List.of(100.2, 0.2));
    List<List<List<Double>>> coordinates = List.of(outer, hole);
    doc.put("coordinates", coordinates);
    Gson gson = new Gson();
    AddDocumentRequest docRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("polygon", MultiValuedField.newBuilder().addValue(gson.toJson(doc)).build())
            .build();
    docs.add(docRequest);
    addDocuments(docs.stream());
  }

  @Test
  public void testGeoPointQuery() {
    GeoPointQuery inGeoPolygonQuery =
        GeoPointQuery.newBuilder()
            .setField("polygon")
            .setPoint(LatLng.newBuilder().setLatitude(0.9).setLongitude(100.9).build())
            .build();
    queryAndVerifyIds(inGeoPolygonQuery, "1");

    // No result in the hole
    GeoPointQuery outGeoPolygonQuery =
        GeoPointQuery.newBuilder()
            .setField("polygon")
            .setPoint(LatLng.newBuilder().setLatitude(0.5).setLongitude(100.5).build())
            .build();
    queryAndVerifyIds(outGeoPolygonQuery);
  }

  @Test
  public void testRetrievePolygon() {
    GeoPointQuery inGeoPolygonQuery =
        GeoPointQuery.newBuilder()
            .setField("polygon")
            .setPoint(LatLng.newBuilder().setLatitude(0.9).setLongitude(100.9).build())
            .build();
    Query query = Query.newBuilder().setGeoPointQuery(inGeoPolygonQuery).build();
    SearchResponse response = doQuery(query, "polygon");
    for (SearchResponse.Hit hit : response.getHitsList()) {
      Struct struct = hit.getFieldsOrThrow("polygon").getFieldValue(0).getStructValue();
      assertEquals("Polygon", struct.getFieldsOrThrow("type").getStringValue());
    }
  }

  private SearchResponse doQuery(Query query, String retrieveFields) {
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

  private void queryAndVerifyIds(GeoPointQuery geoPolygonQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoPointQuery(geoPolygonQuery).build();
    SearchResponse response = doQuery(query, "doc_id");
    List<String> idList = Arrays.asList(expectedIds);
    assertEquals(idList.size(), response.getHitsCount());
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertTrue(idList.contains(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
  }
}
