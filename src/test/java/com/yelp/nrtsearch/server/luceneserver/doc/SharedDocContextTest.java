/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.doc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTaskProvider;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks;
import com.yelp.nrtsearch.server.luceneserver.search.FetchTasks.FetchTask;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.FetchTaskPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.junit.ClassRule;
import org.junit.Test;

public class SharedDocContextTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/doc/SharedContextRegisterFields.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // create a shuffled list of ids
    List<Integer> idList = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; ++i) {
      idList.add(i);
    }
    Collections.shuffle(idList);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (Integer id : idList) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  @Override
  public List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestSharedContextPlugin());
  }

  static class TestSharedContextPlugin extends Plugin implements FetchTaskPlugin {
    TestSharedContextPlugin() {}

    @Override
    public Map<String, FetchTaskProvider<? extends FetchTask>> getFetchTasks() {
      Map<String, FetchTaskProvider<? extends FetchTasks.FetchTask>> taskMap = new HashMap<>();
      taskMap.put("phase_1", Phase1Task::new);
      taskMap.put("phase_2", Phase2Task::new);
      return taskMap;
    }

    static class Phase1Task implements FetchTasks.FetchTask {
      Phase1Task(Map<String, Object> params) {}

      @Override
      public void processHit(
          SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
          throws IOException {
        List<LeafReaderContext> leaves =
            searchContext.getSearcherAndTaxonomy().searcher.getIndexReader().leaves();
        int leafIndex = ReaderUtil.subIndex(hit.getLuceneDocId(), leaves);
        LeafReaderContext leaf = leaves.get(leafIndex);
        SegmentDocLookup segmentDocLookup =
            searchContext.getIndexState().docLookup.getSegmentLookup(leaf);
        segmentDocLookup.setDocId(hit.getLuceneDocId() - leaf.docBase);
        Integer score = (Integer) segmentDocLookup.get("int_score").get(0);
        searchContext
            .getSharedDocContext()
            .getContext(hit.getLuceneDocId())
            .put("shared_info", score * 2);
      }
    }

    static class Phase2Task implements FetchTasks.FetchTask {
      Phase2Task(Map<String, Object> params) {}

      @Override
      public void processHit(
          SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
          throws IOException {
        Integer sharedValue =
            (Integer)
                searchContext
                    .getSharedDocContext()
                    .getContext(hit.getLuceneDocId())
                    .get("shared_info");
        hit.putFields(
            "_context_value",
            CompositeFieldValue.newBuilder()
                .addFieldValue(FieldValue.newBuilder().setIntValue(sharedValue).build())
                .build());
      }
    }
  }

  @Test
  public void testSharedDocContext() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(10)
            .setStartHit(0)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("int_score")
            .setQuery(
                Query.newBuilder()
                    .setRangeQuery(
                        RangeQuery.newBuilder()
                            .setField("int_field")
                            .setLower("20")
                            .setUpper("29")
                            .build())
                    .build())
            .addFetchTasks(
                com.yelp.nrtsearch.server.grpc.FetchTask.newBuilder().setName("phase_1").build())
            .addFetchTasks(
                com.yelp.nrtsearch.server.grpc.FetchTask.newBuilder().setName("phase_2").build())
            .build();
    SearchResponse response = getGrpcServer().getBlockingStub().search(request);
    assertEquals(10, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      assertEquals(
          hit.getFieldsOrThrow("_context_value").getFieldValue(0).getIntValue(),
          hit.getFieldsOrThrow("int_score").getFieldValue(0).getIntValue() * 2);
    }
  }
}
