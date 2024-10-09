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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.ProfileResult.CollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.SegmentStats;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.junit.ClassRule;
import org.junit.Test;

public class SearchStatsWrapperTest extends ServerTestCase {
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
    return getFieldsFromResourceFile("/search/StatsWrapperRegisterFields.json");
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

  @Test
  public void testHasSearchStats() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_score")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder())
                    .setProfile(true)
                    .build());
    assertTrue(searchResponse.getProfileResult().getSearchStats().getCollectorStatsCount() > 1);
    assertTrue(searchResponse.getProfileResult().getSearchStats().getTotalCollectTimeMs() > 0.0);
    assertTrue(searchResponse.getProfileResult().getSearchStats().getTotalReduceTimeMs() > 0.0);
    for (CollectorStats collectorStats :
        searchResponse.getProfileResult().getSearchStats().getCollectorStatsList()) {
      assertFalse(collectorStats.getTerminated());
      assertTrue(collectorStats.getTotalCollectTimeMs() > 0.0);
      assertEquals(50, collectorStats.getTotalCollectedCount());
      assertEquals(0, collectorStats.getAdditionalCollectorStatsCount());
      for (SegmentStats segmentStats : collectorStats.getSegmentStatsList()) {
        assertEquals(10, segmentStats.getMaxDoc());
        assertEquals(10, segmentStats.getNumDocs());
        assertEquals(10, segmentStats.getCollectedCount());
        assertTrue(segmentStats.getCollectTimeMs() > 0.0);
        assertTrue(segmentStats.getRelativeStartTimeMs() > 0.0);
      }
    }
  }

  @Test
  public void testHasQueries() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_score")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder())
                    .setProfile(true)
                    .build());
    assertFalse(searchResponse.getProfileResult().getParsedQuery().isEmpty());
    assertFalse(searchResponse.getProfileResult().getRewrittenQuery().isEmpty());
    assertTrue(searchResponse.getProfileResult().getDrillDownQuery().isEmpty());
  }

  @Test
  public void testHasDrillDownQuery() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(5)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_score")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder())
                    .addFacets(
                        Facet.newBuilder()
                            .setName("test_facet")
                            .setDim("int_score")
                            .setTopN(2)
                            .build())
                    .setProfile(true)
                    .build());
    assertFalse(searchResponse.getProfileResult().getParsedQuery().isEmpty());
    assertFalse(searchResponse.getProfileResult().getRewrittenQuery().isEmpty());
    assertFalse(searchResponse.getProfileResult().getDrillDownQuery().isEmpty());
  }

  public static class MockTerminateCollectorManager
      implements CollectorManager<Collector, TopDocs> {

    @Override
    public Collector newCollector() throws IOException {
      return new MockTerminateCollector();
    }

    @Override
    public TopDocs reduce(Collection<Collector> collectors) throws IOException {
      return null;
    }

    public static class MockTerminateCollector implements Collector {

      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        throw new CollectionTerminatedException();
      }

      @Override
      public ScoreMode scoreMode() {
        return null;
      }
    }
  }

  @Test
  public void testTerminateFlag() throws IOException {
    SearchStatsWrapper searchStatsWrapper =
        new SearchStatsWrapper(new MockTerminateCollectorManager());
    List<Collector> collectors = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      Collector c = searchStatsWrapper.newCollector();
      collectors.add(c);
      try {
        c.getLeafCollector(null);
        assert false;
      } catch (CollectionTerminatedException ignored) {
      }
    }
    searchStatsWrapper.reduce(collectors);
    ProfileResult.Builder builder = ProfileResult.newBuilder();
    searchStatsWrapper.addProfiling(builder);
    ProfileResult result = builder.build();
    assertEquals(3, result.getSearchStats().getCollectorStatsCount());
    for (CollectorStats collectorStats : result.getSearchStats().getCollectorStatsList()) {
      assertTrue(collectorStats.getTerminated());
    }
  }
}
