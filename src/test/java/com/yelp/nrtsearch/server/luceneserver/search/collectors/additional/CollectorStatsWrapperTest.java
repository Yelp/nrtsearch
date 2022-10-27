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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.ProfileResult.AdditionalCollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.CollectorStats;
import com.yelp.nrtsearch.server.grpc.ProfileResult.SegmentStats;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptContext;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptEngine;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class CollectorStatsWrapperTest extends ServerTestCase {
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
    return getFieldsFromResourceFile("/search/collection/CollectorStatsWrapperRegisterFields.json");
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
    return Collections.singletonList(new TestScriptPlugin());
  }

  private static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestScriptEngine());
    }

    private static class TestScriptEngine implements ScriptEngine {

      @Override
      public String getLang() {
        return "test_lang";
      }

      @Override
      public <T> T compile(String source, ScriptContext<T> context) {
        FacetScript.Factory factory = (TestSegmentFactory::new);
        return context.factoryClazz.cast(factory);
      }

      private static class TestSegmentFactory implements FacetScript.SegmentFactory {

        private final Map<String, Object> params;
        private final DocLookup docLookup;

        public TestSegmentFactory(Map<String, Object> params, DocLookup docLookup) {
          this.params = params;
          this.docLookup = docLookup;
        }

        @Override
        public FacetScript newInstance(LeafReaderContext context) throws IOException {
          return new TestScript(params, docLookup, context);
        }

        private static class TestScript extends FacetScript {

          /**
           * FacetScript constructor.
           *
           * @param params script parameters from {@link Script}
           * @param docLookup index level doc values lookup
           * @param leafContext lucene segment context
           */
          public TestScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return getDoc().get("int_field").get(0).toString()
                + getDoc().get("int_score").get(0).toString();
          }
        }
      }
    }
  }

  @Test
  public void testHasAdditionalCollectorStats() {
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
                    .putCollectors(
                        "test_collector1",
                        Collector.newBuilder()
                            .setTerms(
                                TermsCollector.newBuilder()
                                    .setSize(10)
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang("test_lang")
                                            .setSource("test_script")
                                            .build())
                                    .build())
                            .build())
                    .putCollectors(
                        "test_collector2",
                        Collector.newBuilder()
                            .setTerms(
                                TermsCollector.newBuilder()
                                    .setSize(10)
                                    .setScript(
                                        Script.newBuilder()
                                            .setLang("test_lang")
                                            .setSource("test_script")
                                            .build())
                                    .build())
                            .build())
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
      assertEquals(2, collectorStats.getAdditionalCollectorStatsCount());
      AdditionalCollectorStats additionalCollectorStats =
          collectorStats.getAdditionalCollectorStatsOrThrow("test_collector1");
      assertTrue(additionalCollectorStats.getCollectTimeMs() > 0.0);
      additionalCollectorStats =
          collectorStats.getAdditionalCollectorStatsOrThrow("test_collector2");
      assertTrue(additionalCollectorStats.getCollectTimeMs() > 0.0);
      for (SegmentStats segmentStats : collectorStats.getSegmentStatsList()) {
        assertEquals(10, segmentStats.getMaxDoc());
        assertEquals(10, segmentStats.getNumDocs());
        assertEquals(10, segmentStats.getCollectedCount());
        assertTrue(segmentStats.getCollectTimeMs() > 0.0);
        assertTrue(segmentStats.getRelativeStartTimeMs() > 0.0);
      }
    }
  }
}
