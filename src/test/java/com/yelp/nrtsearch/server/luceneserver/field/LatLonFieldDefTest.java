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

import com.google.type.LatLng;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.GeoBoundingBoxQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
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
    queryAndVerifyIds(fremontGeoBoundingBoxQuery, "1");

    GeoBoundingBoxQuery sfGeoBoundingBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_multi")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.793321).setLongitude(-122.430808).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.759250).setLongitude(-122.383569).build())
            .build();
    queryAndVerifyIds(sfGeoBoundingBoxQuery, "2");

    // No results in Mountain View
    GeoBoundingBoxQuery mountainViewGeoBoxQuery =
        GeoBoundingBoxQuery.newBuilder()
            .setField("lat_lon_multi")
            .setTopLeft(
                LatLng.newBuilder().setLatitude(37.409778).setLongitude(-122.116884).build())
            .setBottomRight(
                LatLng.newBuilder().setLatitude(37.346484).setLongitude(-121.984961).build())
            .build();
    queryAndVerifyIds(mountainViewGeoBoxQuery);
  }

  private void queryAndVerifyIds(GeoBoundingBoxQuery geoBoundingBoxQuery, String... expectedIds) {
    Query query = Query.newBuilder().setGeoBoundingBoxQuery(geoBoundingBoxQuery).build();
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
