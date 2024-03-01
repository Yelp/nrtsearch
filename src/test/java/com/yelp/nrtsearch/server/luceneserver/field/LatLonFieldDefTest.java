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
import static org.junit.Assert.fail;

import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class LatLonFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsLatLon.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    // Business in Fremont
    AddDocumentRequest fremontDocRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "lat_lon_multi",
                MultiValuedField.newBuilder()
                    .addValue("37.476220")
                    .addValue("-121.904404")
                    .addValue("37.506900")
                    .addValue("-121.933121")
                    .build())
            .build();
    // Business in SF
    AddDocumentRequest sfDocRequest =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "lat_lon_multi",
                MultiValuedField.newBuilder().addValue("37.781158").addValue("-122.414011").build())
            .build();
    docs.add(fremontDocRequest);
    docs.add(sfDocRequest);
    addDocuments(docs.stream());
  }

  @Test
  public void testGeoBoxQuery() {
    GeoBoundingBoxQuery fremontGeoBoundingBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_multi")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.589207).setLongitude(-122.019474).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.419254).setLongitude(-121.836704).build())
            .build();
    geoBoundingBoxQueryAndVerifyIds(fremontGeoBoundingBoxQuery, "1");

    GeoBoundingBoxQuery sfGeoBoundingBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_multi")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.793321).setLongitude(-122.430808).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.759250).setLongitude(-122.383569).build())
            .build();
    geoBoundingBoxQueryAndVerifyIds(sfGeoBoundingBoxQuery, "2");

    // No results in Mountain View
    GeoBoundingBoxQuery mountainViewGeoBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_multi")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.409778).setLongitude(-122.116884).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.346484).setLongitude(-121.984961).build())
            .build();
    geoBoundingBoxQueryAndVerifyIds(mountainViewGeoBoxQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testGeoBoxQueryNotSearchable() {
    GeoBoundingBoxQuery fremontGeoBoundingBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_not_searchable")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.589207).setLongitude(-122.019474).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.419254).setLongitude(-121.836704).build())
            .build();
    geoBoundingBoxQueryAndVerifyIds(fremontGeoBoundingBoxQuery);
  }

  @Test
  public void testGeoRadiusQuery() {
    GeoRadiusQuery sfGeoRadiusQuery =
        GeoRadiusQuery.newBuilder()
            .setField("lat_lon_multi")
            .setRadius("5 mi")
            .setCenter(
                LatLng.newBuilder().setLatitude(37.7789847).setLongitude(-122.4166709).build())
            .build();
    geoRadiusQueryAndVerifyIds(sfGeoRadiusQuery, "2");

    GeoRadiusQuery fremontGeoRadiusQuery =
        GeoRadiusQuery.newBuilder()
            .setField("lat_lon_multi")
            .setRadius("5 mi")
            .setCenter(
                LatLng.newBuilder().setLatitude(37.4766153).setLongitude(-121.9124278).build())
            .build();
    geoRadiusQueryAndVerifyIds(fremontGeoRadiusQuery, "1");
  }

  @Test
  public void testGeoPolygonQueryNotSearchable() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_not_searchable")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .build())
            .build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("field lat_lon_not_searchable is not searchable"));
    }
  }

  @Test
  public void testGeoPolygonQueryNotPoint() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("doc_id")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .build())
            .build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field doc_id does not support GeoPolygonQuery"));
    }
  }

  @Test
  public void testGeoPolygonQueryNoPolygon() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder().setField("lat_lon_multi").build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("GeoPolygonQuery must contain at least one polygon"));
    }
  }

  @Test
  public void testGeoPolygonQueryNoPoints() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(Polygon.newBuilder().build())
            .build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Polygon must have at least three points"));
    }
  }

  @Test
  public void testGeoPolygonQueryFewPoints() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .build())
            .build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Polygon must have at least three points"));
    }
  }

  @Test
  public void testGeoPolygonQueryFewPointsClosed() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .build())
            .build();
    try {
      geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Closed Polygon must have at least four points"));
    }
  }

  @Test
  public void testGeoPolygonQuery() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery, "2");

    GeoPolygonQuery fremontGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(
                        LatLng.newBuilder().setLatitude(37.51).setLongitude(-121.9331).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.92).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.94).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(fremontGeoPolygonQuery, "1");
  }

  @Test
  public void testGeoPolygonQuery_closed() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery, "2");

    GeoPolygonQuery fremontGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(
                        LatLng.newBuilder().setLatitude(37.51).setLongitude(-121.9331).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.92).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.94).build())
                    .addPoints(
                        LatLng.newBuilder().setLatitude(37.51).setLongitude(-121.9331).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(fremontGeoPolygonQuery, "1");
  }

  @Test
  public void testGeoPolygonQuery_reverseWinding() {
    GeoPolygonQuery sfGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(sfGeoPolygonQuery, "2");

    GeoPolygonQuery fremontGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.94).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.92).build())
                    .addPoints(
                        LatLng.newBuilder().setLatitude(37.51).setLongitude(-121.9331).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(fremontGeoPolygonQuery, "1");
  }

  @Test
  public void testMultiGeoPolygonQuery() {
    GeoPolygonQuery multiGeoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.8).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                    .build())
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(
                        LatLng.newBuilder().setLatitude(37.51).setLongitude(-121.9331).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.92).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.49).setLongitude(-121.94).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(multiGeoPolygonQuery, "1", "2");
  }

  @Test
  public void testGeoPolygonQueryHoles() {
    GeoPolygonQuery geoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQuery, "1", "2");

    GeoPolygonQuery geoPolygonQueryHoles =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .addHoles(
                        Polygon.newBuilder()
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                            .addPoints(
                                LatLng.newBuilder()
                                    .setLatitude(37.8)
                                    .setLongitude(-122.414)
                                    .build())
                            .build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQueryHoles, "1");
  }

  @Test
  public void testGeoPolygonQueryHoles_closed() {
    GeoPolygonQuery geoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQuery, "1", "2");

    GeoPolygonQuery geoPolygonQueryHoles =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addHoles(
                        Polygon.newBuilder()
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                            .addPoints(
                                LatLng.newBuilder()
                                    .setLatitude(37.8)
                                    .setLongitude(-122.414)
                                    .build())
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                            .build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQueryHoles, "1");
  }

  @Test
  public void testGeoPolygonQueryHoles_reverseWinding() {
    GeoPolygonQuery geoPolygonQuery =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQuery, "1", "2");

    GeoPolygonQuery geoPolygonQueryHoles =
        GeoPolygonQuery.newBuilder()
            .setField("lat_lon_multi")
            .addPolygons(
                Polygon.newBuilder()
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-123.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(37.0).setLongitude(-120.0).build())
                    .addPoints(LatLng.newBuilder().setLatitude(38.0).setLongitude(-122.414).build())
                    .addHoles(
                        Polygon.newBuilder()
                            .addPoints(
                                LatLng.newBuilder()
                                    .setLatitude(37.8)
                                    .setLongitude(-122.414)
                                    .build())
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-122.0).build())
                            .addPoints(
                                LatLng.newBuilder().setLatitude(37.6).setLongitude(-123.0).build())
                            .build())
                    .build())
            .build();
    geoPolygonQueryAndVerifyIds(geoPolygonQueryHoles, "1");
  }

  private void geoBoundingBoxQueryAndVerifyIds(
      GeoBoundingBoxQuery geoBoundingBoxQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoBoundingBoxQuery(geoBoundingBoxQuery).build();
    queryAndVerifyIds(query, expectedIds);
  }

  private void geoRadiusQueryAndVerifyIds(GeoRadiusQuery geoRadiusQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoRadiusQuery(geoRadiusQuery).build();
    queryAndVerifyIds(query, expectedIds);
  }

  private void geoPolygonQueryAndVerifyIds(GeoPolygonQuery geoPolygonQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoPolygonQuery(geoPolygonQuery).build();
    queryAndVerifyIds(query, expectedIds);
  }

  private void queryAndVerifyIds(Query query, String... expectedIds) {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(query)
                    .addRetrieveFields("doc_id")
                    .build());
    List<String> idList = Arrays.asList(expectedIds);
    assertEquals(idList.size(), response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      assertTrue(idList.contains(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
  }
}
