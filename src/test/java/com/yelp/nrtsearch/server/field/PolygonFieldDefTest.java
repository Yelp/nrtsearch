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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.gson.Gson;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Polygon;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.document.Document;
import org.junit.ClassRule;
import org.junit.Test;

public class PolygonFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final Gson GSON = new Gson();

  private PolygonfieldDef createFieldDef(Field field) {
    return new PolygonfieldDef(
        "test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsPolygon.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();

    // Doc "1": polygon at lat 0-1, lon 100-101 (with a hole), used by existing tests
    Map<String, Object> doc1 = new HashMap<>();
    doc1.put("type", "Polygon");
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
    doc1.put("coordinates", List.of(outer, hole));
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("polygon", MultiValuedField.newBuilder().addValue(GSON.toJson(doc1)).build())
            .putFields(
                "single_stored", MultiValuedField.newBuilder().addValue(GSON.toJson(doc1)).build())
            .build());

    // Doc "2": polygon at lat 40-41, lon -75 to -74 (roughly New Jersey), used by geo query tests
    docs.add(buildPolygonDoc(name, "2", buildRectPolygon(40.0, -75.0, 41.0, -74.0)));

    addDocuments(docs.stream());
  }

  @Test
  public void testStoredFields() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(3)
                    .addRetrieveFields("single_stored")
                    .addRetrieveFields("single_none_stored")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("1").build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());

    SearchResponse.Hit doc1Hit = response.getHits(0);

    Struct expectedStruct =
        Struct.newBuilder()
            .putFields(
                "coordinates",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.0)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.0)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(101.0)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.0)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(101.0)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(1.0)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.0)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(1.0)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.0)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.0)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .addValues(
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.2)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.2)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.8)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.2)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.8)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.8)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.2)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.8)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .addValues(
                                                Value.newBuilder()
                                                    .setListValue(
                                                        ListValue.newBuilder()
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(100.2)
                                                                    .build())
                                                            .addValues(
                                                                Value.newBuilder()
                                                                    .setNumberValue(0.2)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .putFields("type", Value.newBuilder().setStringValue("Polygon").build())
            .build();

    assertEquals(1, doc1Hit.getFieldsOrThrow("single_stored").getFieldValueCount());
    assertEquals(
        expectedStruct,
        doc1Hit.getFieldsOrThrow("single_stored").getFieldValue(0).getStructValue());
    assertEquals(0, doc1Hit.getFieldsOrThrow("single_none_stored").getFieldValueCount());
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

  @Test
  public void testEmptyIndexing() {
    PolygonfieldDef polygonfieldDef =
        new PolygonfieldDef(
            "polygon",
            Field.newBuilder().setStore(true).setStoreDocValues(true).build(),
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    Document document = new Document();
    polygonfieldDef.parseDocumentField(document, Collections.emptyList(), Collections.emptyList());
    assertEquals(0, document.getFields().size());
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

  // Builds a simple rectangular GeoJSON polygon from (lat1,lon1) to (lat2,lon2)
  private String buildRectPolygon(double lat1, double lon1, double lat2, double lon2) {
    Map<String, Object> doc = new HashMap<>();
    doc.put("type", "Polygon");
    List<List<Double>> ring = new ArrayList<>();
    ring.add(List.of(lon1, lat1));
    ring.add(List.of(lon2, lat1));
    ring.add(List.of(lon2, lat2));
    ring.add(List.of(lon1, lat2));
    ring.add(List.of(lon1, lat1));
    doc.put("coordinates", List.of(ring));
    return GSON.toJson(doc);
  }

  private AddDocumentRequest buildPolygonDoc(String name, String id, String polygonJson) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(name)
        .putFields("doc_id", MultiValuedField.newBuilder().addValue(id).build())
        .putFields("polygon", MultiValuedField.newBuilder().addValue(polygonJson).build())
        .build();
  }

  @Test
  public void testGeoRadiusQuery() {
    // Query centered at (0.5, 100.5) with 100km radius — should intersect doc "1"
    GeoRadiusQuery nearDoc1 =
        GeoRadiusQuery.newBuilder()
            .setField("polygon")
            .setCenter(LatLng.newBuilder().setLatitude(0.5).setLongitude(100.5).build())
            .setRadius("100 km")
            .build();
    queryAndVerifyGeoRadiusIds(nearDoc1, "1");

    // Query centered at (40.5, -74.5) with 100km radius — should intersect doc "2"
    GeoRadiusQuery nearDoc2 =
        GeoRadiusQuery.newBuilder()
            .setField("polygon")
            .setCenter(LatLng.newBuilder().setLatitude(40.5).setLongitude(-74.5).build())
            .setRadius("100 km")
            .build();
    queryAndVerifyGeoRadiusIds(nearDoc2, "2");
  }

  @Test
  public void testGeoRadiusQueryNoIntersection() {
    // Query centered over the Pacific Ocean, far from the test polygon
    GeoRadiusQuery noMatch =
        GeoRadiusQuery.newBuilder()
            .setField("polygon")
            .setCenter(LatLng.newBuilder().setLatitude(0.0).setLongitude(-150.0).build())
            .setRadius("100 km")
            .build();
    queryAndVerifyGeoRadiusIds(noMatch);
  }

  @Test
  public void testGeoRadiusQueryPartialOverlap() {
    // Query circle centered just outside the polygon's east edge (lon=101.1) but large enough
    // to reach back into the polygon (which extends to lon=101.0), verifying INTERSECTS semantics.
    GeoRadiusQuery partialOverlap =
        GeoRadiusQuery.newBuilder()
            .setField("polygon")
            .setCenter(LatLng.newBuilder().setLatitude(0.5).setLongitude(101.1).build())
            .setRadius("50 km")
            .build();
    queryAndVerifyGeoRadiusIds(partialOverlap, "1");
  }

  @Test
  public void testGeoBoundingBoxQueryOnPolygon() {
    // Bounding box that overlaps doc "1" (lat 0-1, lon 100-101)
    GeoBoundingBoxQuery boxForDoc1 =
        GeoBoundingBoxQuery.newBuilder()
            .setField("polygon")
            .setTopLeft(LatLng.newBuilder().setLatitude(2.0).setLongitude(99.0).build())
            .setBottomRight(LatLng.newBuilder().setLatitude(-1.0).setLongitude(102.0).build())
            .build();
    Query q1 = Query.newBuilder().setGeoBoundingBoxQuery(boxForDoc1).build();
    SearchResponse r1 = doQuery(q1, "doc_id");
    assertEquals(1, r1.getHitsCount());
    assertEquals("1", r1.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());

    // Bounding box that does not overlap either polygon
    GeoBoundingBoxQuery noOverlapBox =
        GeoBoundingBoxQuery.newBuilder()
            .setField("polygon")
            .setTopLeft(LatLng.newBuilder().setLatitude(50.0).setLongitude(50.0).build())
            .setBottomRight(LatLng.newBuilder().setLatitude(45.0).setLongitude(60.0).build())
            .build();
    Query qNone = Query.newBuilder().setGeoBoundingBoxQuery(noOverlapBox).build();
    SearchResponse rNone = doQuery(qNone, "doc_id");
    assertEquals(0, rNone.getHitsCount());
  }

  @Test
  public void testGeoPolygonQueryOnPolygon() {
    // Query polygon that intersects doc "1"
    Polygon queryPolygon =
        Polygon.newBuilder()
            .addPoints(LatLng.newBuilder().setLatitude(2.0).setLongitude(99.0).build())
            .addPoints(LatLng.newBuilder().setLatitude(2.0).setLongitude(102.0).build())
            .addPoints(LatLng.newBuilder().setLatitude(-1.0).setLongitude(102.0).build())
            .addPoints(LatLng.newBuilder().setLatitude(-1.0).setLongitude(99.0).build())
            .build();
    GeoPolygonQuery geoPolygonQuery =
        GeoPolygonQuery.newBuilder().setField("polygon").addPolygons(queryPolygon).build();
    Query q = Query.newBuilder().setGeoPolygonQuery(geoPolygonQuery).build();
    SearchResponse r = doQuery(q, "doc_id");
    assertEquals(1, r.getHitsCount());
    assertEquals("1", r.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
  }

  @Test
  public void testGeoRadiusQueryNotSearchable() {
    // "doc_id" is ATOM type, not GeoQueryable — should throw
    GeoRadiusQuery query =
        GeoRadiusQuery.newBuilder()
            .setField("doc_id")
            .setCenter(LatLng.newBuilder().setLatitude(0.5).setLongitude(100.5).build())
            .setRadius("100 km")
            .build();
    try {
      doQuery(Query.newBuilder().setGeoRadiusQuery(query).build(), "doc_id");
      fail("Expected exception");
    } catch (io.grpc.StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("does not support GeoRadiusQuery"));
    }
  }

  private void queryAndVerifyGeoRadiusIds(GeoRadiusQuery geoRadiusQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoRadiusQuery(geoRadiusQuery).build();
    SearchResponse response = doQuery(query, "doc_id");
    List<String> idList = Arrays.asList(expectedIds);
    assertEquals(idList.size(), response.getHitsCount());
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertTrue(idList.contains(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
  }

  @Test
  public void testCreateUpdatedFieldDef() {
    PolygonfieldDef fieldDef =
        createFieldDef(Field.newBuilder().setName("field").setStoreDocValues(true).build());
    FieldDef updatedField =
        fieldDef.createUpdatedFieldDef(
            "field",
            Field.newBuilder().setStoreDocValues(false).build(),
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    assertTrue(updatedField instanceof PolygonfieldDef);
    PolygonfieldDef updatedFieldDef = (PolygonfieldDef) updatedField;

    assertNotSame(fieldDef, updatedFieldDef);
    assertEquals("field", updatedFieldDef.getName());
    assertTrue(fieldDef.hasDocValues());
    assertFalse(updatedFieldDef.hasDocValues());
  }
}
