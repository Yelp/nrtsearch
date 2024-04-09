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
package com.yelp.nrtsearch.server.utils;

import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;

public class MinMaxUtilTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  protected static final Script FIELD_SCRIPT =
      Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("value_field").build();
  protected static final Script SCORE_SCRIPT =
      Script.newBuilder().setLang(JsScriptEngine.LANG).setSource("_score").build();
  protected static final Query MATCH_ALL_SCORE_QUERY =
      Query.newBuilder()
          .setFunctionScoreQuery(
              FunctionScoreQuery.newBuilder()
                  .setQuery(Query.newBuilder().build())
                  .setScript(FIELD_SCRIPT))
          .build();
  protected static final Query MATCH_SOME_SCORE_QUERY =
      Query.newBuilder()
          .setFunctionScoreQuery(
              FunctionScoreQuery.newBuilder()
                  .setQuery(
                      Query.newBuilder()
                          .setTermQuery(
                              TermQuery.newBuilder().setField("int_field").setIntValue(3).build())
                          .build())
                  .setScript(FIELD_SCRIPT))
          .build();
  protected static final Query MATCH_ALL_QUERY =
      Query.newBuilder()
          .setExistsQuery(ExistsQuery.newBuilder().setField("value_field").build())
          .build();
  protected static final Query MATCH_SOME_QUERY =
      Query.newBuilder()
          .setTermQuery(TermQuery.newBuilder().setField("int_field").setIntValue(3).build())
          .build();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/min_max.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder().setSliceMaxSegments(Int32Value.of(1)).build(), false);

    List<AddDocumentRequest> docs = new ArrayList<>();
    int doc_id = 0;
    for (int i = 1; i <= 5; ++i) {
      for (int j = 1; j <= 20; ++j) {
        AddDocumentRequest request =
            AddDocumentRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .putFields(
                    "doc_id",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(Integer.toString(doc_id))
                        .build())
                .putFields(
                    "int_field",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(Integer.toString(i))
                        .build())
                .putFields(
                    "value_field",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(Integer.toString(i * j))
                        .build())
                .build();
        docs.add(request);
        doc_id++;
      }
    }

    // write docs in random order with 10 per segment
    Collections.shuffle(docs);
    for (int i = 0; i < 10; ++i) {
      List<AddDocumentRequest> chunk = new ArrayList<>();
      for (int j = 0; j < 10; ++j) {
        chunk.add(docs.get((10 * i) + j));
      }
      addDocuments(chunk.stream());
      writer.commit();
    }
  }

  protected SearchResponse doQuery(Query query, Collector collector) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setTopHits(10)
                .setQuery(query)
                .putCollectors("test_collector", collector)
                .build());
  }
}
