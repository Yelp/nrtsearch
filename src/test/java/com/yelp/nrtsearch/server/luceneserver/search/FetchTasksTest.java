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
package com.yelp.nrtsearch.server.luceneserver.search;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FetchTask;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.plugins.FetchTaskPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.ClassRule;
import org.junit.Test;

public class FetchTasksTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/FetchTasksRegisterFields.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    for (int i = 0; i < NUM_DOCS; ++i) {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(i + 1))
                      .build())
              .build();
      addDocuments(Stream.of(request));
    }
  }

  @Override
  public List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestFetchTaskPlugin());
  }

  public static class TestFetchTaskPlugin extends Plugin implements FetchTaskPlugin {
    @Override
    public Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> getFetchTasks() {
      Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> taskMap = new HashMap<>();
      taskMap.put("all_hits_task", new TestAllHitsProvider());
      taskMap.put("hits_task", new TestHitsProvider());
      return taskMap;
    }

    public static class TestAllHitsProvider implements FetchTaskProvider<TestAllHitsTask> {
      @Override
      public TestAllHitsTask get(Map<String, Object> params) {
        return new TestAllHitsTask();
      }
    }

    public static class TestAllHitsTask implements FetchTasks.FetchTask {
      @Override
      public void processAllHits(
          SearchContext searchContext, List<SearchResponse.Hit.Builder> hits) {
        for (SearchResponse.Hit.Builder hit : hits) {
          hit.putFields(
              "all_task",
              SearchResponse.Hit.CompositeFieldValue.newBuilder()
                  .addFieldValue(
                      SearchResponse.Hit.FieldValue.newBuilder()
                          .setIntValue(hit.getLuceneDocId() - 10)
                          .build())
                  .build());
        }
      }
    }

    public static class TestHitsProvider implements FetchTaskProvider<TestHitsTask> {
      @Override
      public TestHitsTask get(Map<String, Object> params) {
        return new TestHitsTask();
      }
    }

    public static class TestHitsTask implements FetchTasks.FetchTask {
      @Override
      public void processHit(
          SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
          throws IOException {
        FieldDef fd = searchContext.getQueryFields().get("int_field");
        LoadedDocValues<?> docValues = ((IndexableFieldDef) fd).getDocValues(hitLeaf);
        docValues.setDocId(hit.getLuceneDocId());
        int fieldVal = (Integer) docValues.get(0);
        hit.putFields(
            "hits_task",
            SearchResponse.Hit.CompositeFieldValue.newBuilder()
                .addFieldValue(
                    SearchResponse.Hit.FieldValue.newBuilder().setIntValue(fieldVal + 10).build())
                .build());
      }
    }
  }

  @Test
  public void testNoFetchTask() {
    SearchResponse response = doQuery(Collections.emptyList());
    assertBaseHit(response);
    for (SearchResponse.Hit hit : response.getHitsList()) {
      assertEquals(2, hit.getFieldsCount());
    }
  }

  @Test
  public void testFetchTaskAllHits() {
    FetchTask grpcFetchTask = FetchTask.newBuilder().setName("all_hits_task").build();
    SearchResponse response = doQuery(Collections.singletonList(grpcFetchTask));
    assertBaseHit(response);
    for (int i = 0; i < response.getHitsCount(); ++i) {
      SearchResponse.Hit hit = response.getHits(i);
      assertEquals(3, hit.getFieldsCount());
      assertEquals(
          hit.getLuceneDocId() - 10,
          hit.getFieldsOrThrow("all_task").getFieldValue(0).getIntValue());
    }
  }

  @Test
  public void testFetchTaskHits() {
    FetchTask grpcFetchTask = FetchTask.newBuilder().setName("hits_task").build();
    SearchResponse response = doQuery(Collections.singletonList(grpcFetchTask));
    assertBaseHit(response);
    for (int i = 0; i < response.getHitsCount(); ++i) {
      SearchResponse.Hit hit = response.getHits(i);
      int fieldVal = hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue();
      assertEquals(3, hit.getFieldsCount());
      assertEquals(fieldVal + 10, hit.getFieldsOrThrow("hits_task").getFieldValue(0).getIntValue());
    }
  }

  @Test
  public void testMultipleFetchTasks() {
    FetchTask grpcFetchTask1 = FetchTask.newBuilder().setName("all_hits_task").build();
    FetchTask grpcFetchTask2 = FetchTask.newBuilder().setName("hits_task").build();
    SearchResponse response = doQuery(Arrays.asList(grpcFetchTask1, grpcFetchTask2));
    assertBaseHit(response);
    for (int i = 0; i < response.getHitsCount(); ++i) {
      SearchResponse.Hit hit = response.getHits(i);
      int fieldVal = hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue();
      assertEquals(4, hit.getFieldsCount());
      assertEquals(
          hit.getLuceneDocId() - 10,
          hit.getFieldsOrThrow("all_task").getFieldValue(0).getIntValue());
      assertEquals(fieldVal + 10, hit.getFieldsOrThrow("hits_task").getFieldValue(0).getIntValue());
    }
  }

  private void assertBaseHit(SearchResponse response) {
    assertEquals(NUM_DOCS, response.getHitsCount());
    for (int i = 0; i < response.getHitsCount(); ++i) {
      int expectedId = NUM_DOCS - 1 - i;
      var hit = response.getHits(i);
      assertEquals(
          String.valueOf(expectedId),
          hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
      assertEquals(
          expectedId + 1, hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
      assertEquals(expectedId + 3, hit.getScore(), 0.001);
    }
  }

  private SearchResponse doQuery(List<FetchTask> fetchTasks) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setStartHit(0)
                .setTopHits(NUM_DOCS)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .setQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setQuery(
                                    Query.newBuilder()
                                        .setRangeQuery(
                                            RangeQuery.newBuilder()
                                                .setUpper("10000")
                                                .setLower("0")
                                                .setField("int_field")
                                                .build())
                                        .build())
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource("int_field + 2")
                                        .build())
                                .build())
                        .build())
                .addAllFetchTasks(fetchTasks)
                .build());
  }
}
