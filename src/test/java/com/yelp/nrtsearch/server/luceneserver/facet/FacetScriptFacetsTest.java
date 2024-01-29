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
package com.yelp.nrtsearch.server.luceneserver.facet;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
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

public class FacetScriptFacetsTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static class TestFacetScriptPlugin extends Plugin implements ScriptPlugin {

    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestFacetScriptEngine());
    }

    public static class TestFacetScriptEngine implements ScriptEngine {

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
    return getFieldsFromResourceFile("/facet/facet_script_facets.json");
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

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return Collections.singletonList(new TestFacetScriptPlugin());
  }

  @Test
  public void testFacetScript() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setTopN(6)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        6,
        6,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "2_1")), 16));
  }

  @Test
  public void testFacetLabelSubset() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setTopN(4)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        6,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17));
  }

  @Test
  public void testFacetGreaterN() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        6,
        6,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "0_1", "1_1", "2_0")), 17),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "2_1")), 16));
  }

  @Test
  public void testIterableValue() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        3,
        3,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 33));
  }

  @Test
  public void testIterableLabelSubset() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setTopN(2)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        3,
        2,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66));
  }

  @Test
  public void testFacetScriptRange() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setTopN(6)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        6,
        6,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "2_1", "1_1", "2_0")), 2),
        new ExpectedValues(new HashSet<>(Arrays.asList("1_0", "0_1")), 1));
  }

  @Test
  public void testFacetRangeLabelSubset() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("concat").build())
            .setTopN(4)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        6,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("0_0", "2_1", "1_1", "2_0")), 2));
  }

  @Test
  public void testIterableRangeValue() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setTopN(10)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response,
        10,
        3,
        3,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 4));
  }

  @Test
  public void testIterableRangeLabelSubset() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("combine").build())
            .setTopN(2)
            .build();
    SearchResponse response = doRangeQuery(facet);
    assertResponse(
        response, 10, 3, 2, new ExpectedValues(new HashSet<>(Arrays.asList("0", "1")), 6));
  }

  @Test
  public void testNullValue() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("null").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertEquals(1, response.getFacetResultCount());
    FacetResult result = response.getFacetResult(0);
    assertEquals(100, result.getValue(), 0);
    assertEquals(0, result.getChildCount());
    assertEquals(0, result.getLabelValuesCount());
  }

  @Test
  public void testContainsNull() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("contains_null").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        3,
        3,
        new ExpectedValues(new HashSet<>(Collections.singletonList("0")), 67),
        new ExpectedValues(new HashSet<>(Collections.singletonList("1")), 66),
        new ExpectedValues(new HashSet<>(Collections.singletonList("2")), 33));
  }

  @Test
  public void testNumericValue() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("numeric_value").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        4,
        4,
        new ExpectedValues(new HashSet<>(Arrays.asList("0", "1", "2", "3")), 25));
  }

  @Test
  public void testNumericSet() {
    Facet facet =
        Facet.newBuilder()
            .setScript(Script.newBuilder().setLang("test_lang").setSource("numeric_set").build())
            .setTopN(10)
            .build();
    SearchResponse response = doQuery(facet);
    assertResponse(
        response,
        100,
        3,
        3,
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
      double value,
      int childCount,
      int valuesCount,
      ExpectedValues... expectedValues) {
    assertEquals(1, response.getFacetResultCount());
    FacetResult result = response.getFacetResult(0);
    assertEquals(value, result.getValue(), 0);
    assertEquals(childCount, result.getChildCount());
    assertEquals(valuesCount, result.getLabelValuesCount());

    int sum = 0;
    for (ExpectedValues v : expectedValues) {
      sum += v.labels.size();
    }
    assertEquals(sum, valuesCount);

    int valuesIndex = 0;
    for (ExpectedValues v : expectedValues) {
      Set<String> valueSet = new HashSet<>();
      for (int i = 0; i < v.labels.size(); ++i) {
        valueSet.add(result.getLabelValues(valuesIndex).getLabel());
        assertEquals(v.count, result.getLabelValues(valuesIndex).getValue(), 0);
        valuesIndex++;
      }
      assertEquals(v.labels, valueSet);
    }
  }

  private SearchResponse doQuery(Facet facet) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .addFacets(facet)
                .build());
  }

  private SearchResponse doRangeQuery(Facet facet) {
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
                .addFacets(facet)
                .build());
  }
}
