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
import static org.mockito.Mockito.mock;

import com.google.gson.Gson;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.type.LatLng;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.document.Document;
import org.junit.ClassRule;
import org.junit.Test;

public class PolygonFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

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
            .putFields(
                "single_stored", MultiValuedField.newBuilder().addValue(gson.toJson(doc)).build())
            .build();
    docs.add(docRequest);
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
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(1, response.getHitsCount());

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

    assertEquals(1, response.getHits(0).getFieldsOrThrow("single_stored").getFieldValueCount());
    assertEquals(
        expectedStruct,
        response.getHits(0).getFieldsOrThrow("single_stored").getFieldValue(0).getStructValue());
    assertEquals(
        0, response.getHits(0).getFieldsOrThrow("single_none_stored").getFieldValueCount());
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
