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

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.DoubleTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.FloatTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.IntTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.LongTerms;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery.TextTerms;
import com.yelp.nrtsearch.server.grpc.TermQuery;
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

public class AtomFieldTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsAtom.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    MultiValuedField t1Value = MultiValuedField.newBuilder().addValue("term 1").build();
    MultiValuedField t2Value = MultiValuedField.newBuilder().addValue("term 2").build();
    MultiValuedField t3Value = MultiValuedField.newBuilder().addValue("term 3").build();
    MultiValuedField t12Values =
        MultiValuedField.newBuilder().addValue("term 1").addValue("term 2").build();
    MultiValuedField t23Values =
        MultiValuedField.newBuilder().addValue("term 2").addValue("term 3").build();
    MultiValuedField t31Values =
        MultiValuedField.newBuilder().addValue("term 3").addValue("term 1").build();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("single", t1Value)
            .putFields("multi_one", t2Value)
            .putFields("multi_two", t31Values)
            .putFields("single_stored", t3Value)
            .putFields("multi_stored", t12Values)
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("single", t2Value)
            .putFields("multi_one", t3Value)
            .putFields("multi_two", t12Values)
            .putFields("single_stored", t2Value)
            .putFields("multi_stored", t31Values)
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("3").build())
            .putFields("single", t3Value)
            .putFields("multi_one", t1Value)
            .putFields("multi_two", t23Values)
            .putFields("single_stored", t1Value)
            .putFields("multi_stored", t23Values)
            .build();
    docs.add(request);
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
                    .setTopHits(5)
                    .addRetrieveFields("single_stored")
                    .addRetrieveFields("multi_stored")
                    .addRetrieveFields("single_none_stored")
                    .addRetrieveFields("multi_none_stored")
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(3, response.getHitsCount());
    Hit hit = response.getHits(0);
    assertEquals(1, hit.getFieldsOrThrow("single_stored").getFieldValueCount());
    assertEquals("term 3", hit.getFieldsOrThrow("single_stored").getFieldValue(0).getTextValue());
    assertEquals(2, hit.getFieldsOrThrow("multi_stored").getFieldValueCount());
    assertEquals("term 1", hit.getFieldsOrThrow("multi_stored").getFieldValue(0).getTextValue());
    assertEquals("term 2", hit.getFieldsOrThrow("multi_stored").getFieldValue(1).getTextValue());
    assertEquals(0, hit.getFieldsOrThrow("single_none_stored").getFieldValueCount());
    assertEquals(0, hit.getFieldsOrThrow("multi_none_stored").getFieldValueCount());

    hit = response.getHits(1);
    assertEquals(1, hit.getFieldsOrThrow("single_stored").getFieldValueCount());
    assertEquals("term 2", hit.getFieldsOrThrow("single_stored").getFieldValue(0).getTextValue());
    assertEquals(2, hit.getFieldsOrThrow("multi_stored").getFieldValueCount());
    assertEquals("term 3", hit.getFieldsOrThrow("multi_stored").getFieldValue(0).getTextValue());
    assertEquals("term 1", hit.getFieldsOrThrow("multi_stored").getFieldValue(1).getTextValue());
    assertEquals(0, hit.getFieldsOrThrow("single_none_stored").getFieldValueCount());
    assertEquals(0, hit.getFieldsOrThrow("multi_none_stored").getFieldValueCount());

    hit = response.getHits(2);
    assertEquals(1, hit.getFieldsOrThrow("single_stored").getFieldValueCount());
    assertEquals("term 1", hit.getFieldsOrThrow("single_stored").getFieldValue(0).getTextValue());
    assertEquals(2, hit.getFieldsOrThrow("multi_stored").getFieldValueCount());
    assertEquals("term 2", hit.getFieldsOrThrow("multi_stored").getFieldValue(0).getTextValue());
    assertEquals("term 3", hit.getFieldsOrThrow("multi_stored").getFieldValue(1).getTextValue());
    assertEquals(0, hit.getFieldsOrThrow("single_none_stored").getFieldValueCount());
    assertEquals(0, hit.getFieldsOrThrow("multi_none_stored").getFieldValueCount());
  }

  @Test
  public void testTermQuerySingleValue() {
    queryAndVerifyIds(getTermQuery("single", "term 1"), "1");
    queryAndVerifyIds(getTermQuery("single", "term 2"), "2");
    queryAndVerifyIds(getTermQuery("single", "term 3"), "3");
    queryAndVerifyIds(getTermQuery("single", "term 4"));
  }

  @Test
  public void testTermQuerySingleNoValue() {
    queryAndVerifyIds(getTermQuery("single_none", "term 1"));
    queryAndVerifyIds(getTermQuery("single_none", "term 2"));
    queryAndVerifyIds(getTermQuery("single_none", "term 3"));
    queryAndVerifyIds(getTermQuery("single_none", "term 4"));
  }

  @Test
  public void testTermQueryMultiOne() {
    queryAndVerifyIds(getTermQuery("multi_one", "term 1"), "3");
    queryAndVerifyIds(getTermQuery("multi_one", "term 2"), "1");
    queryAndVerifyIds(getTermQuery("multi_one", "term 3"), "2");
    queryAndVerifyIds(getTermQuery("multi_one", "term 4"));
  }

  @Test
  public void testTermQueryMultiTwo() {
    queryAndVerifyIds(getTermQuery("multi_two", "term 1"), "1", "2");
    queryAndVerifyIds(getTermQuery("multi_two", "term 2"), "2", "3");
    queryAndVerifyIds(getTermQuery("multi_two", "term 3"), "3", "1");
    queryAndVerifyIds(getTermQuery("multi_two", "term 4"));
  }

  @Test
  public void testTermQueryMultiNone() {
    queryAndVerifyIds(getTermQuery("multi_none", "term 1"));
    queryAndVerifyIds(getTermQuery("multi_none", "term 2"));
    queryAndVerifyIds(getTermQuery("multi_none", "term 3"));
    queryAndVerifyIds(getTermQuery("multi_none", "term 4"));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryBoolTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").setBooleanValue(true).build();

    queryAndVerifyIds(termQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryIntTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").setIntValue(0).build();

    queryAndVerifyIds(termQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryLongTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").setLongValue(0).build();

    queryAndVerifyIds(termQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryFloatTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").setFloatValue(0).build();

    queryAndVerifyIds(termQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryDoubleTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").setDoubleValue(0).build();

    queryAndVerifyIds(termQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryUnsetTerm() {
    TermQuery termQuery = TermQuery.newBuilder().setField("single").build();

    queryAndVerifyIds(termQuery);
  }

  @Test
  public void testTermInSetQuerySingleValue() {
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 1"), "1");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 2"), "2");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 3"), "3");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 4"));

    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 1", "term 2"), "1", "2");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 2", "term 3"), "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 3", "term 1"), "3", "1");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 3", "term 4"), "3");
    queryInSetAndVerifyIds(getTermInSetQuery("single", "term 4", "term 5"));
  }

  @Test
  public void testTermInSetQuerySingleNoValue() {
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 1"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 2"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 3"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 4"));

    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 1", "term 2"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 2", "term 3"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 3", "term 1"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 3", "term 4"));
    queryInSetAndVerifyIds(getTermInSetQuery("single_none", "term 4", "term 5"));
  }

  @Test
  public void testTermInSetQueryMultiOne() {
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 1"), "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 2"), "1");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 3"), "2");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 4"));

    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 1", "term 2"), "3", "1");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 2", "term 3"), "1", "2");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 3", "term 1"), "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 3", "term 4"), "2");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_one", "term 4", "term 5"));
  }

  @Test
  public void testTermInSetQueryMultiTwo() {
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 1"), "1", "2");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 2"), "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 3"), "3", "1");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 4"));

    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 1", "term 2"), "1", "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 2", "term 3"), "1", "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 3", "term 1"), "1", "2", "3");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 3", "term 4"), "3", "1");
    queryInSetAndVerifyIds(getTermInSetQuery("multi_two", "term 4", "term 5"));
  }

  @Test
  public void testTermInSetQueryMultiNone() {
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 1"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 2"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 3"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 4"));

    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 1", "term 2"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 2", "term 3"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 3", "term 1"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 3", "term 4"));
    queryInSetAndVerifyIds(getTermInSetQuery("multi_none", "term 4", "term 5"));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetQueryIntTerms() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single")
            .setIntTerms(IntTerms.newBuilder().addTerms(0).build())
            .build();

    queryInSetAndVerifyIds(termInSetQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetQueryLongTerms() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single")
            .setLongTerms(LongTerms.newBuilder().addTerms(0).build())
            .build();

    queryInSetAndVerifyIds(termInSetQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetQueryFloatTerms() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single")
            .setFloatTerms(FloatTerms.newBuilder().addTerms(0).build())
            .build();

    queryInSetAndVerifyIds(termInSetQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetQueryDoubleTerms() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single")
            .setDoubleTerms(DoubleTerms.newBuilder().addTerms(0).build())
            .build();

    queryInSetAndVerifyIds(termInSetQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetQueryUnsetTerms() {
    TermInSetQuery termInSetQuery = TermInSetQuery.newBuilder().setField("single").build();

    queryInSetAndVerifyIds(termInSetQuery);
  }

  private TermQuery getTermQuery(String field, String term) {
    return TermQuery.newBuilder().setField(field).setTextValue(term).build();
  }

  private TermInSetQuery getTermInSetQuery(String field, String... terms) {
    return TermInSetQuery.newBuilder()
        .setField(field)
        .setTextTerms(TextTerms.newBuilder().addAllTerms(Arrays.asList(terms)).build())
        .build();
  }

  private void queryAndVerifyIds(TermQuery termQuery, String... expectedIds) {
    Query query = Query.newBuilder().setTermQuery(termQuery).build();
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

  private void queryInSetAndVerifyIds(TermInSetQuery termInSetQuery, String... expectedIds) {
    Query query = Query.newBuilder().setTermInSetQuery(termInSetQuery).build();
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
