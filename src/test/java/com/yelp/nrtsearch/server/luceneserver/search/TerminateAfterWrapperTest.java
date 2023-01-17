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
package com.yelp.nrtsearch.server.luceneserver.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class TerminateAfterWrapperTest extends ServerTestCase {
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
    return getFieldsFromResourceFile("/search/TerminateAfterRegisterFields.json");
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

  @Test
  public void testNoTerminateAfter() {
    SearchResponse response = doQuery(0, 0.0, false);
    assertEquals(100, response.getHitsCount());
    assertFalse(response.getTerminatedEarly());
  }

  @Test
  public void testTerminateAfter() {
    SearchResponse response = doQuery(10, 0.0, false);
    assertEquals(10, response.getHitsCount());
    assertTrue(response.getTerminatedEarly());
  }

  @Test
  public void testDefaultTerminateAfter() throws IOException {
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    try {
      setDefaultTerminateAfter(15);
      SearchResponse response = doQuery(0, 0.0, false);
      assertEquals(15, response.getHitsCount());
      assertTrue(response.getTerminatedEarly());
    } finally {
      setDefaultTerminateAfter(0);
    }
  }

  @Test
  public void testOverrideDefaultTerminateAfter() throws IOException {
    IndexState indexState = getGlobalState().getIndex(DEFAULT_TEST_INDEX);
    try {
      setDefaultTerminateAfter(15);
      SearchResponse response = doQuery(5, 0.0, false);
      assertEquals(5, response.getHitsCount());
      assertTrue(response.getTerminatedEarly());
    } finally {
      setDefaultTerminateAfter(0);
    }
  }

  @Test
  public void testWithOtherWrappers() {
    SearchResponse response = doQuery(10, 1000.0, true);
    assertEquals(10, response.getHitsCount());
    assertTrue(response.getTerminatedEarly());
  }

  private void setDefaultTerminateAfter(int defaultTerminateAfter) throws IOException {
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder()
                .setDefaultTerminateAfter(
                    Int32Value.newBuilder().setValue(defaultTerminateAfter).build())
                .build());
  }

  private SearchResponse doQuery(int terminateAfter, double timeout, boolean profile) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_score")
                .addRetrieveFields("int_field")
                .setTerminateAfter(terminateAfter)
                .setProfile(profile)
                .setTimeoutSec(timeout)
                .setStartHit(0)
                .setTopHits(100)
                .build());
  }
}
