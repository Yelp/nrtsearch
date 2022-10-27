/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.PluginCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.CollectorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.junit.ClassRule;
import org.junit.Test;

public class CollectorTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int NUM_BIZ = 3;
  private static final int SEGMENT_CHUNK = 10;
  private static final String COLLECTOR_NAME = "top_reviews_aggregation";
  private static final String PLUGIN_COLLECTOR = "plugin_collector";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/CollectorRegisterFields.json");
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
                  "review_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "business_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % NUM_BIZ))
                      .build())
              .putFields(
                  "review_count",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class TestCollectorPlugin extends Plugin implements CollectorPlugin {
    TestCollectorPlugin() {}

    @Override
    public Map<
            String,
            CollectorProvider<
                ? extends
                    AdditionalCollectorManager<
                        ? extends org.apache.lucene.search.Collector, CollectorResult>>>
        getCollectors() {
      Map<
              String,
              CollectorProvider<
                  ? extends
                      AdditionalCollectorManager<
                          ? extends org.apache.lucene.search.Collector, CollectorResult>>>
          collectorProviderMap = new HashMap<>();
      collectorProviderMap.put(PLUGIN_COLLECTOR, TopHitsCollectorManager::new);
      return collectorProviderMap;
    }

    /** Collects the top n reviews for each business_id * */
    public static class TopHitsCollectorManager
        implements AdditionalCollectorManager<
            TopHitsCollectorManager.TopHitsCollector, CollectorResult> {

      public final String name;
      public final CollectorCreatorContext context;
      public final Integer size;
      private final IndexableFieldDef businessIdField;
      private SearchContext searchContext;

      public TopHitsCollectorManager(
          String name,
          CollectorCreatorContext context,
          Map<String, Object> params,
          Map<
                  String,
                  Supplier<
                      AdditionalCollectorManager<
                          ? extends org.apache.lucene.search.Collector, CollectorResult>>>
              nestedCollectorSuppliers) {
        this.name = name;
        this.context = context;
        // The maximum number of top matching reviews to return per business.
        this.size = ((Number) params.get("size")).intValue();
        this.businessIdField = (IndexableFieldDef) context.getIndexState().getField("business_id");
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public void setSearchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
      }

      @Override
      public TopHitsCollector newCollector() {
        return new TopHitsCollector();
      }

      /**
       * Merges the collector results into a final list of top n reviews per business and builds
       * them as a {@link SearchResponse} in the {@link CollectorResult}. Uses the {@link
       * SearchContext} to determine which fields to retrieve
       */
      @Override
      public CollectorResult reduce(Collection<TopHitsCollector> collectors) throws IOException {
        List<Map.Entry<Integer, Float>> topHits = getTopHits(collectors);
        SearchResponse.Builder searchResponseBuilder = SearchResponse.newBuilder();
        // Build response hits from (docId, score)
        for (Map.Entry<Integer, Float> entry : topHits) {
          searchResponseBuilder
              .addHitsBuilder()
              .setLuceneDocId(entry.getKey())
              .setScore(entry.getValue());
        }
        List<SearchResponse.Hit.Builder> hitBuilders =
            new ArrayList<>(searchResponseBuilder.getHitsBuilderList());
        hitBuilders.sort(Comparator.comparing(SearchResponse.Hit.Builder::getLuceneDocId));
        // Fetch fields for each hit
        SearchHandler.FillDocsTask fillDocsTask =
            new SearchHandler.FillDocsTask(searchContext, hitBuilders);
        fillDocsTask.run();
        hitBuilders.sort(
            Comparator.comparing(SearchResponse.Hit.Builder::getScore, Comparator.reverseOrder()));
        return CollectorResult.newBuilder()
            .setAnyResult(Any.pack(searchResponseBuilder.build()))
            .build();
      }

      /** Merge collector results into a final list of top hits * */
      private List<Map.Entry<Integer, Float>> getTopHits(Collection<TopHitsCollector> collectors) {
        if (collectors.isEmpty()) {
          return Collections.emptyList();
        }
        Map<Integer, PriorityQueue<Map.Entry<Integer, Float>>> globalTopHitsByBusiness =
            new HashMap<>();
        // Merge priority queues for each business, maintaining top hits
        for (TopHitsCollector collector : collectors) {
          for (Map.Entry<Integer, PriorityQueue<Map.Entry<Integer, Float>>> entry :
              collector.topHitsByBusiness.entrySet()) {
            for (Map.Entry<Integer, Float> docIdAndScore : entry.getValue()) {
              Integer businessId = entry.getKey();
              if (!globalTopHitsByBusiness.containsKey(businessId)) {
                globalTopHitsByBusiness.put(
                    businessId, new PriorityQueue<>(size, Map.Entry.comparingByValue()));
              }
              PriorityQueue<Map.Entry<Integer, Float>> queue =
                  globalTopHitsByBusiness.get(businessId);
              queue.add(docIdAndScore);
              if (queue.size() > size) {
                queue.poll();
              }
            }
          }
        }
        return globalTopHitsByBusiness.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      }

      public class TopHitsCollector implements org.apache.lucene.search.Collector {
        // Local (segment-level) top n doc_ids per business_id
        Map<Integer, PriorityQueue<Map.Entry<Integer, Float>>> topHitsByBusiness = new HashMap<>();

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
          return new TopHitsLeafCollector(context);
        }

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE;
        }

        public class TopHitsLeafCollector implements LeafCollector {
          private final LeafReaderContext leafContext;
          private final LoadedDocValues.SingleInteger businessIdDocValues;
          private Scorable scorerForCurrentDoc;

          public TopHitsLeafCollector(LeafReaderContext leafContext) {
            this.leafContext = leafContext;
            try {
              this.businessIdDocValues =
                  (LoadedDocValues.SingleInteger) businessIdField.getDocValues(leafContext);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void setScorer(Scorable scorer) throws IOException {
            scorerForCurrentDoc = scorer;
          }

          /** Get top n doc_ids for each business_id. Uses a priority queue to maintain top n * */
          @Override
          public void collect(int doc) throws IOException {
            businessIdDocValues.setDocId(doc);
            Integer businessId = businessIdDocValues.getValue();
            if (businessId != null) {
              if (!topHitsByBusiness.containsKey(businessId)) {
                topHitsByBusiness.put(
                    businessId, new PriorityQueue<>(size, Map.Entry.comparingByValue()));
              }
              PriorityQueue<Map.Entry<Integer, Float>> queue = topHitsByBusiness.get(businessId);
              queue.add(
                  new AbstractMap.SimpleEntry<>(
                      doc + leafContext.docBase, scorerForCurrentDoc.score()));
              if (queue.size() > size) {
                queue.poll();
              }
            }
          }
        }
      }
    }
  }

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestCollectorPlugin());
  }

  @Test
  public void testUnknownCollector() {
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(1)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("review_id")
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder().setLang("js").setSource("review_count").build())
                            .build())
                    .build())
            .putCollectors(
                COLLECTOR_NAME,
                com.yelp.nrtsearch.server.grpc.Collector.newBuilder()
                    .setPluginCollector(PluginCollector.newBuilder().setName("invalid").build())
                    .build())
            .build();
    try {
      getGrpcServer().getBlockingStub().search(request);
      fail();
    } catch (StatusRuntimeException e) {
      String expectedMessage =
          "Invalid collector name: invalid, must be one of: [plugin_collector]";
      assertTrue(e.getMessage().contains(expectedMessage));
    }
  }

  /** Collect the top 3 reviews for each business * */
  @Test
  public void testCustomCollector() throws InvalidProtocolBufferException {
    // topHits = 0 results in java.lang.IllegalArgumentException:
    // numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count
    SearchRequest request =
        SearchRequest.newBuilder()
            .setTopHits(1)
            .setIndexName(DEFAULT_TEST_INDEX)
            .addRetrieveFields("review_id")
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setScript(
                                Script.newBuilder().setLang("js").setSource("review_count").build())
                            .build())
                    .build())
            .putCollectors(
                COLLECTOR_NAME,
                Collector.newBuilder()
                    .setPluginCollector(
                        PluginCollector.newBuilder()
                            .setName(PLUGIN_COLLECTOR)
                            .setParams(
                                Struct.newBuilder()
                                    .putFields(
                                        "size", Value.newBuilder().setNumberValue(3).build()))
                            .build())
                    .build())
            .build();
    Map<String, CollectorResult> collectorsResult =
        getGrpcServer().getBlockingStub().search(request).getCollectorResultsMap();
    assertEquals(1, collectorsResult.size());
    // review_id -> score
    Map<String, Double> expectedHits =
        Map.of(
            "0", 100.0,
            "1", 99.0,
            "2", 98.0,
            "3", 97.0,
            "4", 96.0,
            "5", 95.0,
            "6", 94.0,
            "7", 93.0,
            "8", 92.0);
    Map<String, Double> collectedHits =
        collectorsResult.get(COLLECTOR_NAME).getAnyResult().unpack(SearchResponse.class)
            .getHitsList().stream()
            .collect(
                Collectors.toMap(
                    hit -> hit.getFieldsMap().get("review_id").getFieldValue(0).getTextValue(),
                    SearchResponse.Hit::getScore));
    assertEquals(expectedHits, collectedHits);
  }
}
