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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript.SegmentFactory;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptContext;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptEngine;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class TermsCollectorManagerTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static class TestTermsScriptPlugin extends Plugin implements ScriptPlugin {

    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestTermsScriptEngine());
    }

    public static class TestTermsScriptEngine implements ScriptEngine {

      @Override
      public String getLang() {
        return "test_lang";
      }

      @Override
      public <T> T compile(String source, ScriptContext<T> context) {
        return context.factoryClazz.cast(new TestFactory(source));
      }

      public static class TestFactory implements FacetScript.Factory {
        private final String source;

        public TestFactory(String source) {
          this.source = source;
        }

        @Override
        public SegmentFactory newFactory(Map<String, Object> params, DocLookup docLookup) {
          return new TestSegmentFactory(params, docLookup, source);
        }
      }

      public static class TestSegmentFactory implements FacetScript.SegmentFactory {
        private final Map<String, Object> params;
        private final DocLookup docLookup;
        private final String source;

        public TestSegmentFactory(Map<String, Object> params, DocLookup docLookup, String source) {
          this.params = params;
          this.docLookup = docLookup;
          this.source = source;
        }

        @Override
        public FacetScript newInstance(LeafReaderContext context) {
          switch (source) {
            case "concat":
              return new ConcatScript(params, docLookup, context);
            case "combine":
              return new CombineScript(params, docLookup, context);
            case "null":
              return new NullScript(params, docLookup, context);
            case "contains_null":
              return new ContainsNullScript(params, docLookup, context);
            case "numeric_value":
              return new NumericScript(params, docLookup, context);
            case "numeric_set":
              return new NumericSetScript(params, docLookup, context);
          }
          throw new IllegalStateException("Unsupported script source: " + source);
        }

        public static class ConcatScript extends FacetScript {

          public ConcatScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return getDoc().get("atom_1").get(0) + "_" + getDoc().get("atom_2").get(0);
          }
        }

        public static class CombineScript extends FacetScript {

          public CombineScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            Set<Object> combineSet = new HashSet<>();
            combineSet.add(getDoc().get("atom_1").get(0));
            combineSet.add(getDoc().get("atom_2").get(0));
            return combineSet;
          }
        }

        public static class NullScript extends FacetScript {

          public NullScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return null;
          }
        }

        public static class ContainsNullScript extends FacetScript {

          public ContainsNullScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            Set<Object> combineSet = new HashSet<>();
            combineSet.add(getDoc().get("atom_1").get(0));
            combineSet.add(null);
            combineSet.add(getDoc().get("atom_2").get(0));
            return combineSet;
          }
        }

        public static class NumericScript extends FacetScript {

          public NumericScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return ((Number) getDoc().get("int_field").get(0)).intValue() % 4;
          }
        }

        public static class NumericSetScript extends FacetScript {

          public NumericSetScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            Set<Object> combineSet = new HashSet<>();
            combineSet.add(Integer.parseInt((String) getDoc().get("atom_1").get(0)));
            combineSet.add(Integer.parseInt((String) getDoc().get("atom_2").get(0)));
            return combineSet;
          }
        }
      }
    }
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/collection/terms_script.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    // add documents one chunk at a time to ensure multiple index segments
    List<AddDocumentRequest> requestChunk = new ArrayList<>();
    for (int id = 0; id < NUM_DOCS; ++id) {
      requestChunk.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "atom_1",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 3))
                      .build())
              .putFields(
                  "atom_2",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id % 2))
                      .build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  protected List<Plugin> getPlugins() {
    return Collections.singletonList(new TestTermsScriptPlugin());
  }

  @Test
  public void testTermsScript() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setSize(6)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        6,
        6,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "2_1")), 16));
  }

  @Test
  public void testScriptBucketSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setSize(4)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        6,
        4,
        32,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17));
  }

  @Test
  public void testScriptGreaterSize() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        6,
        6,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "2_1")), 16));
  }

  @Test
  public void testScriptIterableValue() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 33));
  }

  @Test
  public void testScriptIterableBucketSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setSize(2)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        2,
        33,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66));
  }

  @Test
  public void testTermsScriptRange() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setSize(6)
            .build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        6,
        6,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "2_1", "1_1", "2_0")), 2),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "0_1")), 1));
  }

  @Test
  public void testScriptRangeBucketSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setSize(4)
            .build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        6,
        4,
        2,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "2_1", "1_1", "2_0")), 2));
  }

  @Test
  public void testScriptIterableRangeValue() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setSize(10)
            .build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 4));
  }

  @Test
  public void testScriptIterableRangeBucketSubset() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setSize(2)
            .build();
    SearchResponse response = doRangeQuery(terms);
    assertResponse(
        response, 3, 2, 4, new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6));
  }

  @Test
  public void testScriptNullValue() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("null").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(0, result.getBucketsCount());
    assertEquals(0, result.getTotalBuckets());
    assertEquals(0, result.getTotalOtherCounts());
  }

  @Test
  public void testScriptContainsNull() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("contains_null").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 33));
  }

  @Test
  public void testScriptNumericValue() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("numeric_value").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        4,
        4,
        0,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1", "2", "3")), 25));
  }

  @Test
  public void testScriptNumericSet() {
    TermsCollector terms =
        TermsCollector.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("numeric_set").build())
            .setSize(10)
            .build();
    SearchResponse response = doQuery(terms);
    assertResponse(
        response,
        3,
        3,
        0,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 33));
  }

  private static class ExpectedValues {
    public Set<String> labels;
    public double count;

    public ExpectedValues(Set<String> labels, double count) {
      this.labels = labels;
      this.count = count;
    }
  }

  private void assertResponse(
      SearchResponse response,
      int totalBuckets,
      int bucketCount,
      int otherCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getCollectorResultsCount());
    BucketResult result = response.getCollectorResultsOrThrow("test_collector").getBucketResult();
    assertEquals(totalBuckets, result.getTotalBuckets());
    assertEquals(bucketCount, result.getBucketsCount());
    assertEquals(otherCount, result.getTotalOtherCounts());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, bucketCount);

    int valuesIndex = 0;
    for (ExpectedValues v : expectedValues) {
      Set<String> valueSet = new HashSet<>();
      for (int i = 0; i < v.labels.size(); ++i) {
        valueSet.add(result.getBuckets(valuesIndex).getKey());
        assertEquals(v.count, result.getBuckets(valuesIndex).getCount(), 0);
        valuesIndex++;
      }
      assertEquals(v.labels, valueSet);
    }
  }

  private SearchResponse doQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }

  private SearchResponse doRangeQuery(TermsCollector terms) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(
                    Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower("41")
                                .setUpper("50")
                                .build())
                        .build())
                .putCollectors("test_collector", Collector.newBuilder().setTerms(terms).build())
                .build());
  }
}
