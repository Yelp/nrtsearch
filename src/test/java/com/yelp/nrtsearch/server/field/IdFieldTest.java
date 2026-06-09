/*
 * Copyright 2024 Yelp Inc.
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

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

public class IdFieldTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private IdFieldDef createFieldDef(Field field) {
    return new IdFieldDef("test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsIdStored.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> requestList = new ArrayList<>();
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("2").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("3").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("4").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("5").build())
            .build());
    requestList.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("6").build())
            .build());
    addDocuments(requestList.stream());
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
                    .addRetrieveFields("id")
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(3, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("1", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());

    hit = response.getHits(1);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("2", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());

    hit = response.getHits(2);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("3", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
  }

  @Test
  public void testAlwaysSearchable() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(3)
                    .addRetrieveFields("id")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("id").setTextValue("2").build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    SearchResponse.Hit hit = response.getHits(0);
    assertEquals(1, hit.getFieldsOrThrow("id").getFieldValueCount());
    assertEquals("2", hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
  }

  @Test
  public void testRangeQuery() {
    String fieldName = "id";
    // Both bounds defined

    // Both inclusive
    RangeQuery rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setLower("2").setUpper("5").build();
    assertRangeQuery(rangeQuery, "2", "3", "4", "5");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("2")
            .setUpper("5")
            .setLowerExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4", "5");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("2")
            .setUpper("5")
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "2", "3", "4");

    // Both exclusive
    rangeQuery =
        RangeQuery.newBuilder()
            .setField(fieldName)
            .setLower("2")
            .setUpper("5")
            .setLowerExclusive(true)
            .setUpperExclusive(true)
            .build();
    assertRangeQuery(rangeQuery, "3", "4");

    // Only upper bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(fieldName).setUpper("4").build();
    assertRangeQuery(rangeQuery, "1", "2", "3", "4");

    // Lower inclusive, upper exclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setUpper("4").setUpperExclusive(true).build();
    assertRangeQuery(rangeQuery, "1", "2", "3");

    // Only lower bound defined

    // Both inclusive
    rangeQuery = RangeQuery.newBuilder().setField(fieldName).setLower("3").build();
    assertRangeQuery(rangeQuery, "3", "4", "5", "6");

    // Lower exclusive, upper inclusive
    rangeQuery =
        RangeQuery.newBuilder().setField(fieldName).setLower("3").setLowerExclusive(true).build();
    assertRangeQuery(rangeQuery, "4", "5", "6");
  }

  private void assertRangeQuery(RangeQuery rangeQuery, String... expectedValues) {
    Query query = Query.newBuilder().setRangeQuery(rangeQuery).build();
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(query)
                    .addRetrieveFields("id")
                    .build());
    assertEquals(expectedValues.length, searchResponse.getHitsCount());
    List<String> actualValues =
        searchResponse.getHitsList().stream()
            .map(hit -> hit.getFieldsMap().get("id").getFieldValueList().get(0).getTextValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.asList(expectedValues), actualValues);
  }

  @Test
  public void testCreateUpdatedFieldDef() {
    IdFieldDef fieldDef =
        createFieldDef(Field.newBuilder().setName("field").setStoreDocValues(true).build());
    FieldDef updatedField =
        fieldDef.createUpdatedFieldDef(
            "field",
            Field.newBuilder().setStoreDocValues(false).setStore(true).build(),
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    assertTrue(updatedField instanceof IdFieldDef);
    IdFieldDef updatedFieldDef = (IdFieldDef) updatedField;

    assertNotSame(fieldDef, updatedFieldDef);
    assertEquals("field", updatedFieldDef.getName());
    assertTrue(fieldDef.hasDocValues());
    assertFalse(updatedFieldDef.hasDocValues());
  }
}
