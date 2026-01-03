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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiSegmentTest extends ServerTestCase {
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
    return getFieldsFromResourceFile("/registerFieldsMultiSegment.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
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

  // precondition for other tests
  @Test
  public void testNumSegments() throws Exception {
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = getGlobalState().getIndexOrThrow(TEST_INDEX).getShard(0).acquire();
      assertEquals(NUM_DOCS / SEGMENT_CHUNK, s.searcher().getIndexReader().leaves().size());
      for (LeafReaderContext context : s.searcher().getIndexReader().leaves()) {
        assertEquals(SEGMENT_CHUNK, context.reader().maxDoc());
      }
    } finally {
      if (s != null) {
        getGlobalState().getIndexOrThrow(TEST_INDEX).getShard(0).release(s);
      }
    }
  }

  @Test
  public void testMultipleSegments() {
    // range query through index with all strides
    for (int i = 0; i < NUM_DOCS; ++i) {
      int start = 0;
      while (start < NUM_DOCS) {
        SearchResponse searchResponse =
            getGrpcServer()
                .getBlockingStub()
                .search(
                    SearchRequest.newBuilder()
                        .setIndexName(TEST_INDEX)
                        .setStartHit(0)
                        .setTopHits(NUM_DOCS)
                        .addRetrieveFields("doc_id")
                        .addRetrieveFields("int_score")
                        .addRetrieveFields("int_field")
                        .setQuery(
                            Query.newBuilder()
                                .setFunctionScoreQuery(
                                    FunctionScoreQuery.newBuilder()
                                        .setScript(
                                            Script.newBuilder()
                                                .setLang("js")
                                                .setSource("int_score")
                                                .build())
                                        .setQuery(
                                            Query.newBuilder()
                                                .setRangeQuery(
                                                    RangeQuery.newBuilder()
                                                        .setField("int_field")
                                                        .setLower(String.valueOf(start))
                                                        .setUpper(String.valueOf(start + i))
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build());
        int expectedHits;
        if (start + i >= NUM_DOCS) {
          expectedHits = NUM_DOCS - start;
        } else {
          expectedHits = i + 1;
        }
        assertEquals(expectedHits, searchResponse.getHitsCount());

        for (int j = 0; j < searchResponse.getHitsCount(); ++j) {
          int expectedId = start + j;
          int expectedScore = NUM_DOCS - expectedId;

          var hit = searchResponse.getHits(j);
          String hitId = hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
          assertEquals(String.valueOf(expectedId), hitId);
          int scoreVal = hit.getFieldsOrThrow("int_score").getFieldValue(0).getIntValue();
          assertEquals(expectedScore, scoreVal);
          int fieldVal = hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue();
          assertEquals(expectedId, fieldVal);
        }

        start += (i + 1);
      }
    }
  }

  @Test
  public void testExplain() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(NUM_DOCS)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_score")
                    .addRetrieveFields("int_field")
                    .setQuery(
                        Query.newBuilder()
                            .setFunctionScoreQuery(
                                FunctionScoreQuery.newBuilder()
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang("js")
                                            .setSource("int_score")
                                            .build())
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("int_field")
                                                    .setLower(String.valueOf(0))
                                                    .setUpper(String.valueOf(NUM_DOCS / 2))
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .setExplain(true)
                    .build());
    for (int i = 0; i < searchResponse.getHitsCount(); i++) {
      var hit = searchResponse.getHits(i);
      int fieldVal = hit.getFieldsOrThrow("int_field").getFieldValue(0).getIntValue();
      assertEquals(i, fieldVal);
      var explain = hit.getExplain();
      var expectedExplain =
          String.format(
              "%d.0 = weight(FunctionScoreQuery(IndexOrDocValuesQuery(indexQuery=int_field:[0 TO 50], dvQuery=int_field:[0 TO 50]), scored by expr(int_score))), result of:\n"
                  + "  %<d.0 = int_score, computed from:\n"
                  + "    %<d.0 = double(int_score)",
              NUM_DOCS - i);
      assertEquals(expectedExplain, explain.trim());
    }
  }
}
