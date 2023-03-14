/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.analysis;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class MappingV2CharFilterFactoryITest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/analysis/registerFieldsCharFilter.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("text_field", MultiValuedField.newBuilder().addValue("?tarm's1").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("text_field", MultiValuedField.newBuilder().addValue("tar?m&2").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testMappingV2Filter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("text_field")
                                    .setTextValue("term1")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals(
        "1", response.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());

    response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("text_field")
                                    .setTextValue("term 2")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals(
        "2", response.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
  }
}
