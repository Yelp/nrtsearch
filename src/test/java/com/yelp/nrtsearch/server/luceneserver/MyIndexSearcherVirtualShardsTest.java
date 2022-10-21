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
package com.yelp.nrtsearch.server.luceneserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MyIndexSearcherVirtualShardsTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/VirtualShardsRegisterFields.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
  }

  @Override
  public String getExtraConfig() {
    return "virtualSharding: true";
  }

  @Before
  public void clearIndex() throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;
    writer.deleteAll();
  }

  @Test
  public void testVirtualShards() throws Exception {
    setLiveSettings(4, 10000, 100);
    addSegments(Arrays.asList(10, 10, 10, 10, 10, 10, 10));
    assertSlices(Arrays.asList(20, 20, 20, 10), Arrays.asList(2, 2, 2, 1));
  }

  @Test
  public void testLessVirtualShards() throws Exception {
    setLiveSettings(4, 10000, 100);
    addSegments(Arrays.asList(10, 10));
    assertSlices(Arrays.asList(10, 10), Arrays.asList(1, 1));
  }

  @Test
  public void testNoVirtualShards() throws Exception {
    setLiveSettings(4, 10000, 100);
    addSegments(Collections.emptyList());
    assertSlices(Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testUnevenSegments() throws Exception {
    setLiveSettings(3, 10000, 100);
    addSegments(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertSlices(Arrays.asList(16, 15, 14), Arrays.asList(3, 3, 3));
  }

  @Test
  public void testMaxDocs() throws Exception {
    setLiveSettings(2, 25, 100);
    addSegments(Arrays.asList(10, 10, 10, 10, 10, 10, 10, 10, 10));
    assertSlices(Arrays.asList(30, 30, 20, 10), Arrays.asList(3, 3, 2, 1));
  }

  @Test
  public void testMaxSegments() throws Exception {
    setLiveSettings(3, 10000, 2);
    addSegments(Arrays.asList(10, 10, 10, 10, 10, 10, 5, 4, 3));
    assertSlices(Arrays.asList(20, 20, 20, 5, 4, 3), Arrays.asList(2, 2, 2, 1, 1, 1));
  }

  @Test
  public void testHasVirtualShards() throws Exception {
    setLiveSettings(111, 10000, 2);
    addSegments(Collections.emptyList());
    SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    try {
      s = shardState.acquire();
      assertTrue(s.searcher instanceof MyIndexSearcher);
      MyIndexSearcher searcher = (MyIndexSearcher) s.searcher;
      assertTrue(searcher.getExecutor() instanceof MyIndexSearcher.ExecutorWithParams);
      MyIndexSearcher.ExecutorWithParams params =
          (MyIndexSearcher.ExecutorWithParams) searcher.getExecutor();
      assertEquals(111, params.virtualShards);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private void setLiveSettings(int virtualShards, int maxDocs, int maxSegments) throws IOException {
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder()
                .setVirtualShards(Int32Value.newBuilder().setValue(virtualShards).build())
                .setSliceMaxDocs(Int32Value.newBuilder().setValue(maxDocs).build())
                .setSliceMaxSegments(Int32Value.newBuilder().setValue(maxSegments).build())
                .build());
  }

  private void addSegments(Iterable<Integer> sizes) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).writer;

    int currentVal = 0;
    for (Integer size : sizes) {
      List<AddDocumentRequest> requestChunk = new ArrayList<>();
      for (int i = 0; i < size; ++i) {
        requestChunk.add(
            AddDocumentRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .putFields(
                    "doc_id",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(String.valueOf(currentVal))
                        .build())
                .putFields(
                    "int_score",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(String.valueOf(currentVal + 1))
                        .build())
                .putFields(
                    "int_field",
                    AddDocumentRequest.MultiValuedField.newBuilder()
                        .addValue(String.valueOf(currentVal + 2))
                        .build())
                .build());
        currentVal++;
      }
      addDocuments(requestChunk.stream());
      writer.commit();
    }
    getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).maybeRefreshBlocking();
  }

  private void assertSlices(List<Integer> docCounts, List<Integer> segmentCounts)
      throws IOException {
    assertEquals(docCounts.size(), segmentCounts.size());

    SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0);
    try {
      s = shardState.acquire();
      LeafSlice[] slices = s.searcher.getSlices();
      assertEquals(docCounts.size(), slices.length);
      for (int i = 0; i < docCounts.size(); ++i) {
        assertEquals(segmentCounts.get(i), Integer.valueOf(slices[i].leaves.length));

        int totalDocs = 0;
        for (int j = 0; j < slices[i].leaves.length; ++j) {
          totalDocs += slices[i].leaves[j].reader().numDocs();
        }
        assertEquals(docCounts.get(i), Integer.valueOf(totalDocs));
      }
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }
}
