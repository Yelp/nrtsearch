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
package com.yelp.nrtsearch.server.facet;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.script.RuntimeScript;
import com.yelp.nrtsearch.server.script.RuntimeScript.SegmentFactory;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class RuntimeScriptFacetsTest extends ServerTestCase {
  private static final int NUM_DOCS = 100;
  private static final int TOP_HITS = 10;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static class TestRuntimeScriptPlugin extends Plugin implements ScriptPlugin {

    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Collections.singletonList(new TestRuntimeScriptEngine());
    }

    public static class TestRuntimeScriptEngine implements ScriptEngine {

      @Override
      public String getLang() {
        return "test_p";
      }

      @Override
      public <T> T compile(String source, ScriptContext<T> context) {
        return context.factoryClazz.cast(new TestFactory(source));
      }

      public static class TestFactory implements RuntimeScript.Factory {
        private final String source;

        public TestFactory(String source) {
          this.source = source;
        }

        @Override
        public SegmentFactory newFactory(Map<String, Object> params, DocLookup docLookup) {
          return new TestSegmentFactory(params, docLookup, source);
        }
      }

      public static class TestSegmentFactory implements RuntimeScript.SegmentFactory {
        private final Map<String, Object> params;
        private final DocLookup docLookup;
        private final String source;

        public TestSegmentFactory(Map<String, Object> params, DocLookup docLookup, String source) {
          this.params = params;
          this.docLookup = docLookup;
          this.source = source;
        }

        @Override
        public RuntimeScript newInstance(LeafReaderContext context) {
          switch (source) {
            case "int":
              return new IntScript(params, docLookup, context);
            case "string":
              return new StringScript(params, docLookup, context);
            case "map":
              return new MapScript(params, docLookup, context);
            case "list":
              return new ListScript(params, docLookup, context);
            case "docValue":
              return new DocValueScript(params, docLookup, context);
          }
          throw new IllegalStateException("Unsupported script source: " + source);
        }

        public static class IntScript extends RuntimeScript {

          public IntScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return 2;
          }
        }

        public static class StringScript extends RuntimeScript {

          public StringScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return "2";
          }
        }

        public static class MapScript extends RuntimeScript {

          public MapScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            return Collections.singletonMap("key", 2.0);
          }
        }

        public static class ListScript extends RuntimeScript {

          public ListScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            List nums = new ArrayList();
            nums.add("1");
            nums.add("2");
            return nums;
          }
        }

        public static class DocValueScript extends RuntimeScript {

          public DocValueScript(
              Map<String, Object> params, DocLookup docLookup, LeafReaderContext leafContext) {
            super(params, docLookup, leafContext);
          }

          @Override
          public Object execute() {
            Map<String, LoadedDocValues<?>> doc = getDoc();
            return doc.get("atom_1").get(0) + "_" + doc.get("atom_2").get(0);
          }
        }
      }
    }
  }

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/script/runtime_field_script.json");
  }

  protected void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndexOrThrow(name).getShard(0).writer;
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

      if (requestChunk.size() == TOP_HITS) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  @Override
  protected List<Plugin> getPlugins(NrtsearchConfig configuration) {
    return Collections.singletonList(new TestRuntimeScriptPlugin());
  }

  @Test
  public void RuntimeScriptForInt() {

    RuntimeField runtimeField =
        RuntimeField.newBuilder()
            .setScript(Script.newBuilder().setLang("test_p").setSource("int").build())
            .setName("runtime_field")
            .build();

    List expectedValues = new ArrayList<>();
    for (int id = 0; id < TOP_HITS; ++id) {
      expectedValues.add(2);
    }
    SearchResponse response = doQuery(runtimeField);
    assertEquals(TOP_HITS, response.getHitsCount());
    for (int id = 0; id < TOP_HITS; ++id) {
      assertEquals(
          response.getHits(id).getFieldsMap().get("runtime_field").getFieldValue(0).getIntValue(),
          expectedValues.get(id));
    }
  }

  @Test
  public void RuntimeScriptForString() {

    RuntimeField runtimeField =
        RuntimeField.newBuilder()
            .setScript(Script.newBuilder().setLang("test_p").setSource("string").build())
            .setName("runtime_field")
            .build();

    List expectedValues = new ArrayList<>();
    for (int id = 0; id < TOP_HITS; ++id) {
      expectedValues.add("2");
    }
    SearchResponse response = doQuery(runtimeField);
    assertEquals(TOP_HITS, response.getHitsCount());
    for (int id = 0; id < TOP_HITS; ++id) {
      assertEquals(
          response.getHits(id).getFieldsMap().get("runtime_field").getFieldValue(0).getTextValue(),
          expectedValues.get(id));
    }
  }

  @Test
  public void RuntimeScriptForMap() {

    RuntimeField runtimeField =
        RuntimeField.newBuilder()
            .setScript(Script.newBuilder().setLang("test_p").setSource("map").build())
            .setName("runtime_field")
            .build();

    List expectedValues = new ArrayList<>();
    for (int id = 0; id < TOP_HITS; ++id) {
      expectedValues.add(2.0);
    }
    SearchResponse response = doQuery(runtimeField);
    assertEquals(TOP_HITS, response.getHitsCount());
    for (int id = 0; id < TOP_HITS; ++id) {
      assertEquals(
          response
              .getHits(id)
              .getFieldsMap()
              .get("runtime_field")
              .getFieldValue(0)
              .getStructValue()
              .getFieldsMap()
              .get("key")
              .getNumberValue(),
          expectedValues.get(id));
    }
  }

  @Test
  public void RuntimeScriptForList() {

    RuntimeField runtimeField =
        RuntimeField.newBuilder()
            .setScript(Script.newBuilder().setLang("test_p").setSource("list").build())
            .setName("runtime_field")
            .build();

    List<List<Value.Builder>> expectedValues = new ArrayList<>();
    for (int id = 0; id < TOP_HITS; ++id) {
      List<Value.Builder> nums = new ArrayList();
      nums.add(Value.newBuilder().setStringValue("1"));
      nums.add(Value.newBuilder().setStringValue("2"));
      List<Value.Builder> values = Collections.unmodifiableList(nums);
      expectedValues.add(values);
    }
    SearchResponse response = doQuery(runtimeField);
    assertEquals(TOP_HITS, response.getHitsCount());
    for (int id = 0; id < TOP_HITS; ++id) {
      String respOne =
          response
              .getHits(id)
              .getFieldsMap()
              .get("runtime_field")
              .getFieldValueList()
              .get(0)
              .getListValue()
              .getValuesList()
              .get(0)
              .getStringValue();
      String respTwo =
          response
              .getHits(id)
              .getFieldsMap()
              .get("runtime_field")
              .getFieldValueList()
              .get(0)
              .getListValue()
              .getValuesList()
              .get(1)
              .getStringValue();
      assertEquals(respOne, expectedValues.get(id).get(0).getStringValue());
      assertEquals(respTwo, expectedValues.get(id).get(1).getStringValue());
    }
  }

  @Test
  public void RuntimeScriptForDocValue() {

    RuntimeField runtimeField =
        RuntimeField.newBuilder()
            .setScript(Script.newBuilder().setLang("test_p").setSource("docValue").build())
            .setName("runtime_field")
            .build();

    List expectedValues = new ArrayList<>();
    for (int id = 0; id < TOP_HITS; ++id) {
      expectedValues.add(String.valueOf(id % 3) + "_" + String.valueOf(id % 2));
    }
    SearchResponse response = doQuery(runtimeField);
    assertEquals(TOP_HITS, response.getHitsCount());
    for (int id = 0; id < TOP_HITS; id++) {
      assertEquals(
          response.getHits(id).getFieldsMap().get("runtime_field").getFieldValue(0).getTextValue(),
          expectedValues.get(id));
    }
  }

  private SearchResponse doQuery(RuntimeField runtimeField) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setStartHit(0)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("atom_1")
                .addRetrieveFields("atom_2")
                .addRetrieveFields("runtime_field")
                .addRuntimeFields(runtimeField)
                .build());
  }
}
