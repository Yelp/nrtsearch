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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.LeafCollector;
import org.junit.Test;

public class MaxValueCollectorManagerTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @Override
  public FieldDefRequest getIndexDef(String name) throws Exception {
    return getFieldsFromResourceFile("/collectors/MaxValueCollector.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    int count = 0;
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
                      .addValue(String.valueOf((i + 10) % NUM_DOCS))
                      .build())
              .putFields(
                  "long_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 66) % NUM_DOCS) * 2))
                      .build())
              .putFields(
                  "float_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 33) % NUM_DOCS) * 1.25))
                      .build())
              .putFields(
                  "double_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(((i + 90) % NUM_DOCS) * 2.75))
                      .build())
              .build();
      addDocuments(Stream.of(request));
      count++;
      if ((count % SEGMENT_CHUNK) == 0) {
        writer.commit();
      }
    }
  }

  public static class DummySearchContext implements SearchContext {
    Map<String, FieldDef> queryFields;

    public DummySearchContext(Map<String, FieldDef> queryFields) {
      this.queryFields = queryFields;
    }

    @Override
    public IndexState indexState() {
      return null;
    }

    @Override
    public ShardState shardState() {
      return null;
    }

    @Override
    public SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy() {
      return null;
    }

    @Override
    public Optional<DrillSideways> maybeDrillSideways() {
      return Optional.empty();
    }

    @Override
    public SearchResponse.Builder searchResponse() {
      return null;
    }

    @Override
    public long timestampSec() {
      return 0;
    }

    @Override
    public int startHit() {
      return 0;
    }

    @Override
    public Map<String, FieldDef> queryFields() {
      return queryFields;
    }

    @Override
    public Map<String, FieldDef> retrieveFields() {
      return null;
    }

    @Override
    public org.apache.lucene.search.Query query() {
      return null;
    }

    @Override
    public SearchCollectorManager collectorManager() {
      return null;
    }
  }

  @Test
  public void testIntMaxValue() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder().build())
                    .putCollectors(
                        "int_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("int_field").build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getCollectorResultsCount());
    assertEquals(99, searchResponse.getCollectorResultsOrThrow("int_max").getMax().getIntValue());
  }

  @Test
  public void testLongMaxValue() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder().build())
                    .putCollectors(
                        "long_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("long_field").build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getCollectorResultsCount());
    assertEquals(
        198, searchResponse.getCollectorResultsOrThrow("long_max").getMax().getLongValue());
  }

  @Test
  public void testFloatMaxValue() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder().build())
                    .putCollectors(
                        "float_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("float_field").build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getCollectorResultsCount());
    assertEquals(
        123.75,
        searchResponse.getCollectorResultsOrThrow("float_max").getMax().getFloatValue(),
        0.001);
  }

  @Test
  public void testDoubleMaxValue() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder().build())
                    .putCollectors(
                        "double_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("double_field").build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getCollectorResultsCount());
    assertEquals(
        272.25,
        searchResponse.getCollectorResultsOrThrow("double_max").getMax().getDoubleValue(),
        0.001);
  }

  @Test
  public void testMultipleMaxValue() {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .setQuery(Query.newBuilder().build())
                    .putCollectors(
                        "int_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("int_field").build())
                            .build())
                    .putCollectors(
                        "long_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("long_field").build())
                            .build())
                    .putCollectors(
                        "float_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("float_field").build())
                            .build())
                    .putCollectors(
                        "double_max",
                        Collector.newBuilder()
                            .setMax(MaxCollector.newBuilder().setField("double_field").build())
                            .build())
                    .build());
    assertEquals(4, searchResponse.getCollectorResultsCount());
    assertEquals(99, searchResponse.getCollectorResultsOrThrow("int_max").getMax().getIntValue());
    assertEquals(
        198, searchResponse.getCollectorResultsOrThrow("long_max").getMax().getLongValue());
    assertEquals(
        123.75,
        searchResponse.getCollectorResultsOrThrow("float_max").getMax().getFloatValue(),
        0.001);
    assertEquals(
        272.25,
        searchResponse.getCollectorResultsOrThrow("double_max").getMax().getDoubleValue(),
        0.001);
  }

  @Test
  public void testReduction() throws Exception {
    MaxCollector collectorDesc = MaxCollector.newBuilder().setField("long_field").build();
    Map<String, FieldDef> indexFields =
        getGrpcServer().getGlobalState().getIndex(DEFAULT_TEST_INDEX).getAllFields();
    SearchContext searchContext = new DummySearchContext(indexFields);
    MaxValueCollectorManager collectorManager =
        new MaxValueCollectorManager(searchContext, "my_max", collectorDesc);
    List<MaxValueCollectorManager.MaxValueCollector> collectors = new ArrayList<>();

    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).acquire();
      assertEquals(NUM_DOCS / SEGMENT_CHUNK, s.searcher.getIndexReader().leaves().size());
      for (LeafReaderContext context : s.searcher.getIndexReader().leaves()) {
        MaxValueCollectorManager.MaxValueCollector collector = collectorManager.newCollector();
        LeafCollector leafCollector = collector.getLeafCollector(context);
        for (int i = 0; i < context.reader().maxDoc(); ++i) {
          leafCollector.collect(i);
        }
        collectors.add(collector);
      }
      CollectorResult result = collectorManager.reduce(collectors);
      assertEquals(CollectorResult.CollectorResultsCase.MAX, result.getCollectorResultsCase());
      assertEquals(198, result.getMax().getLongValue());
    } finally {
      if (s != null) {
        getGlobalState().getIndex(DEFAULT_TEST_INDEX).getShard(0).release(s);
      }
    }
  }
}
