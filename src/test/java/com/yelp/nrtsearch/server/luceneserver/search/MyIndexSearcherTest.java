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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.index.IndexState;
import com.yelp.nrtsearch.server.luceneserver.index.ShardState;
import com.yelp.nrtsearch.server.luceneserver.search.MyIndexSearcher.ExecutorWithParams;
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
import org.junit.ClassRule;
import org.junit.Test;

public class MyIndexSearcherTest extends ServerTestCase {
  private static final String DOCS_INDEX = "test_index_docs";
  private static final String SEGMENTS_INDEX = "test_index_segments";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Arrays.asList(DOCS_INDEX, SEGMENTS_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    if (DOCS_INDEX.equals(name)) {
      return getFieldsFromResourceFile("/search/IndexSearcherDocsRegisterFields.json");
    } else if (SEGMENTS_INDEX.equals(name)) {
      return getFieldsFromResourceFile("/search/IndexSearcherSegmentsRegisterFields.json");
    }
    throw new IllegalArgumentException("Unknown index: " + name);
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
  public LiveSettingsRequest getLiveSettings(String name) {
    if (DOCS_INDEX.equals(name)) {
      return LiveSettingsRequest.newBuilder()
          .setIndexName(name)
          .setSliceMaxDocs(25)
          .setSliceMaxSegments(10)
          .build();
    } else if (SEGMENTS_INDEX.equals(name)) {
      return LiveSettingsRequest.newBuilder()
          .setIndexName(name)
          .setSliceMaxDocs(1000)
          .setSliceMaxSegments(4)
          .build();
    }
    throw new IllegalArgumentException("Unknown index: " + name);
  }

  @Test
  public void testHasSliceParams() throws IOException {
    assertSliceParams(DOCS_INDEX, 25, 10);
    assertSliceParams(SEGMENTS_INDEX, 1000, 4);
  }

  private void assertSliceParams(String index, int maxDocs, int maxSegments) throws IOException {
    SearcherAndTaxonomy s = null;
    IndexState indexState = getGlobalState().getIndex(index);
    ShardState shardState = indexState.getShard(0);
    try {
      s = shardState.acquire();
      assertTrue(s.searcher instanceof MyIndexSearcher);
      MyIndexSearcher searcher = (MyIndexSearcher) s.searcher;
      assertTrue(searcher.getExecutor() instanceof MyIndexSearcher.ExecutorWithParams);
      MyIndexSearcher.ExecutorWithParams params =
          (MyIndexSearcher.ExecutorWithParams) searcher.getExecutor();
      assertSame(indexState.getSearchThreadPoolExecutor(), params.wrapped);
      assertEquals(maxDocs, params.sliceMaxDocs);
      assertEquals(maxSegments, params.sliceMaxSegments);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testSliceDocsLimit() throws IOException {
    SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(DOCS_INDEX).getShard(0);
    try {
      s = shardState.acquire();
      LeafSlice[] slices = s.searcher.getSlices();
      assertEquals(4, slices.length);
      assertEquals(3, slices[0].leaves.length);
      assertEquals(3, slices[1].leaves.length);
      assertEquals(3, slices[2].leaves.length);
      assertEquals(1, slices[3].leaves.length);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test
  public void testSliceSegmentsLimit() throws IOException {
    SearcherAndTaxonomy s = null;
    ShardState shardState = getGlobalState().getIndex(SEGMENTS_INDEX).getShard(0);
    try {
      s = shardState.acquire();
      LeafSlice[] slices = s.searcher.getSlices();
      assertEquals(3, slices.length);
      assertEquals(4, slices[0].leaves.length);
      assertEquals(4, slices[1].leaves.length);
      assertEquals(2, slices[2].leaves.length);
    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullWrappedExecutor() throws IOException {
    new ExecutorWithParams(null, 10, 10, 1);
  }
}
