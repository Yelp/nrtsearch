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

public class BooleanFieldTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsBoolean.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    MultiValuedField trueValue = MultiValuedField.newBuilder().addValue("true").build();
    MultiValuedField falseValue = MultiValuedField.newBuilder().addValue("false").build();
    MultiValuedField bothValues =
        MultiValuedField.newBuilder().addValue("true").addValue("false").build();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("single", trueValue)
            .putFields("single_not_search", trueValue)
            .putFields("multi_one", falseValue)
            .putFields("multi_both", bothValues)
            .putFields("multi_not_search", falseValue)
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("single", falseValue)
            .putFields("single_not_search", falseValue)
            .putFields("multi_one", trueValue)
            .putFields("multi_both", bothValues)
            .putFields("multi_not_search", trueValue)
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testTermQuerySingleValue() {
    TermQuery falseBoolQuery =
        TermQuery.newBuilder().setField("single").setBooleanValue(false).build();
    TermQuery trueBoolQuery =
        TermQuery.newBuilder().setField("single").setBooleanValue(true).build();

    queryAndVerifyIds(trueBoolQuery, "1");
    queryAndVerifyIds(falseBoolQuery, "2");
  }

  public void testTermQuerySingleValueBooleanStrings() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").setTextValue("false").build();
    TermQuery trueQuery = TermQuery.newBuilder().setField("single").setTextValue("true").build();

    queryAndVerifyIds(trueQuery, "1");
    queryAndVerifyIds(falseQuery, "2");
  }

  @Test
  public void testTermQuerySingleNoValue() {
    TermQuery falseBoolQuery =
        TermQuery.newBuilder().setField("single_none").setBooleanValue(false).build();
    TermQuery trueBoolQuery =
        TermQuery.newBuilder().setField("single_none").setBooleanValue(true).build();
    queryAndVerifyIds(trueBoolQuery);
    queryAndVerifyIds(falseBoolQuery);
  }

  public void testTermQuerySingleNoValueBooleanStrings() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("single_none").setTextValue("false").build();
    TermQuery trueQuery =
        TermQuery.newBuilder().setField("single_none").setTextValue("true").build();
    queryAndVerifyIds(trueQuery);
    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQuerySingleNotSearchable() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("single_not_search").setTextValue("false").build();

    queryAndVerifyIds(falseQuery);
  }

  @Test
  public void testTermQueryMultiOne() {
    TermQuery falseBoolQuery =
        TermQuery.newBuilder().setField("multi_one").setBooleanValue(false).build();
    TermQuery trueBoolQuery =
        TermQuery.newBuilder().setField("multi_one").setBooleanValue(true).build();

    queryAndVerifyIds(trueBoolQuery, "2");
    queryAndVerifyIds(falseBoolQuery, "1");
  }

  public void testTermQueryMultiOneBooleanStrings() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("multi_one").setTextValue("false").build();
    TermQuery trueQuery = TermQuery.newBuilder().setField("multi_one").setTextValue("true").build();

    queryAndVerifyIds(trueQuery, "2");
    queryAndVerifyIds(falseQuery, "1");
  }

  @Test
  public void testTermQueryMultiBoth() {
    TermQuery falseBoolQuery =
        TermQuery.newBuilder().setField("multi_both").setBooleanValue(false).build();
    TermQuery trueBoolQuery =
        TermQuery.newBuilder().setField("multi_both").setBooleanValue(true).build();

    queryAndVerifyIds(trueBoolQuery, "1", "2");
    queryAndVerifyIds(falseBoolQuery, "1", "2");
  }

  public void testTermQueryMultiBothBooleanStrings() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("multi_both").setTextValue("false").build();
    TermQuery trueQuery =
        TermQuery.newBuilder().setField("multi_both").setTextValue("true").build();

    queryAndVerifyIds(trueQuery, "1", "2");
    queryAndVerifyIds(falseQuery, "1", "2");
  }

  @Test
  public void testTermQueryMultiNone() {
    TermQuery falseBoolQuery =
        TermQuery.newBuilder().setField("multi_none").setBooleanValue(false).build();
    TermQuery trueBoolQuery =
        TermQuery.newBuilder().setField("multi_none").setBooleanValue(true).build();

    queryAndVerifyIds(trueBoolQuery);
    queryAndVerifyIds(falseBoolQuery);
  }

  public void testTermQueryMultiNoneBooleanStrings() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("multi_none").setTextValue("false").build();
    TermQuery trueQuery =
        TermQuery.newBuilder().setField("multi_none").setTextValue("true").build();

    queryAndVerifyIds(trueQuery);
    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryMultiNotSearchable() {
    TermQuery falseQuery =
        TermQuery.newBuilder().setField("multi_not_search").setTextValue("false").build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryIntTerm() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").setIntValue(0).build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryLongTerm() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").setLongValue(0).build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryFloatTerm() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").setFloatValue(0).build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryDoubleTerm() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").setDoubleValue(0).build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermQueryUnsetTerm() {
    TermQuery falseQuery = TermQuery.newBuilder().setField("single").build();

    queryAndVerifyIds(falseQuery);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTermInSetNotSupported() {
    TermInSetQuery termInSetQuery =
        TermInSetQuery.newBuilder()
            .setField("single")
            .setTextTerms(TextTerms.newBuilder().addTerms("true").addTerms("false").build())
            .build();
    getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(Query.newBuilder().setTermInSetQuery(termInSetQuery).build())
                .addRetrieveFields("doc_id")
                .build());
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
}
