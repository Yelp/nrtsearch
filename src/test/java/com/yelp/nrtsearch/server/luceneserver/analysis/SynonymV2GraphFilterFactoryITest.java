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

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SynonymV2GraphFilterFactoryITest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    AnalyzerCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/analysis/registerFieldsSynonymTokenFilter.json");
  }

  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    AddDocumentRequest request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("text_field", MultiValuedField.newBuilder().addValue("plaza").build())
            .build();
    docs.add(request);
    request =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields("doc_id", MultiValuedField.newBuilder().addValue("2").build())
            .putFields("text_field", MultiValuedField.newBuilder().addValue("str").build())
            .build();
    docs.add(request);
    addDocuments(docs.stream());
  }

  @Test
  public void testSynonymV2GraphFilter() {
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
                                    .setTextValue("plaça")
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
                                    .setTextValue("straße")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals(
        "2", response.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
  }
}
