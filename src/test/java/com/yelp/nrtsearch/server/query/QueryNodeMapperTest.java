/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.ScriptPlugin;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import com.yelp.nrtsearch.server.script.ScriptService;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.search.SearchRequestProcessor;
import io.grpc.testing.GrpcCleanupRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DoubleValues;
import org.junit.ClassRule;
import org.junit.Test;

public class QueryNodeMapperTest extends ServerTestCase {
  private static final String TEST_INDEX = "test_index";
  private static final int NUM_DOCS = 100;
  private static final int SEGMENT_CHUNK = 10;
  private static final List<String> RETRIEVE_LIST =
      Arrays.asList("doc_id", "int_score", "int_field");

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private void init(List<Plugin> plugins) {
    ScriptService.initialize(getEmptyConfig(), plugins);
  }

  private NrtsearchConfig getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new NrtsearchConfig(new ByteArrayInputStream(config.getBytes()));
  }

  @Override
  public List<String> getIndices() {
    return Collections.singletonList(TEST_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/registerFieldsDocLookup.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
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
                      .addValue(String.valueOf(NUM_DOCS - id))
                      .build())
              .putFields(
                  "int_score",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "int_field",
                  AddDocumentRequest.MultiValuedField.newBuilder()
                      .addValue(String.valueOf(id))
                      .build())
              .putFields(
                  "sorted_doc_values_facet_field",
                  AddDocumentRequest.MultiValuedField.newBuilder().addValue("0").build())
              .build());

      if (requestChunk.size() == SEGMENT_CHUNK) {
        addDocuments(requestChunk.stream());
        requestChunk.clear();
        writer.commit();
      }
    }
  }

  static class TestScriptPlugin extends Plugin implements ScriptPlugin {
    @Override
    public Iterable<ScriptEngine> getScriptEngines(List<ScriptContext<?>> contexts) {
      return Arrays.asList(new TestScriptEngine());
    }
  }

  public static class TestScriptEngine implements ScriptEngine {

    @Override
    public String getLang() {
      return "test_lang";
    }

    @Override
    public <T> T compile(String source, ScriptContext<T> context) {
      ScoreScript.Factory factory =
          ((params, docLookup) -> new TestScriptFactory(params, docLookup, source));
      return context.factoryClazz.cast(factory);
    }
  }

  static class TestScriptFactory extends ScoreScript.SegmentFactory {
    private final Map<String, Object> params;
    private final DocLookup docLookup;
    private final String scriptId;

    public TestScriptFactory(Map<String, Object> params, DocLookup docLookup, String scriptId) {
      super(params, docLookup);
      this.params = params;
      this.docLookup = docLookup;
      this.scriptId = scriptId;
    }

    @Override
    public boolean needs_score() {
      return true;
    }

    @Override
    public DoubleValues newInstance(LeafReaderContext ctx, DoubleValues scores) {
      return new DoubleScoreScript(params, docLookup, ctx, scores);
    }

    public DocLookup getDocLookup() {
      return docLookup;
    }
  }

  static class DoubleScoreScript extends ScoreScript {
    public DoubleScoreScript(
        Map<String, Object> params,
        DocLookup docLookup,
        LeafReaderContext context,
        DoubleValues scores) {
      super(params, docLookup, context, scores);
    }

    @Override
    public double execute() {
      return 2.4;
    }
  }

  @Test
  public void testFunctionScoreQueryField() throws IOException {
    init(Collections.singletonList(new TestScriptPlugin()));
    SearchRequest request =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setTopHits(NUM_DOCS)
            .addAllRetrieveFields(RETRIEVE_LIST)
            .addAllVirtualFields(getVirtualFields())
            .setQuery(getQuery())
            .build();
    IndexState indexState = getGlobalState().getIndexOrThrow(TEST_INDEX);
    ShardState shardState = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;

    try {
      s = shardState.acquire();
      SearchContext context =
          SearchRequestProcessor.buildContextForRequest(
              request, indexState, shardState, s, Diagnostics.newBuilder(), null);
      org.apache.lucene.queries.function.FunctionScoreQuery query =
          (org.apache.lucene.queries.function.FunctionScoreQuery) context.getQuery();
      assertNotNull(query);
      TestScriptFactory source = (TestScriptFactory) query.getSource();
      assertEquals(source.getDocLookup().getFieldDef("int_field").getName(), "int_field");
      assertEquals(source.getDocLookup().getFieldDef("v1").getName(), "v1");

    } finally {
      if (s != null) {
        shardState.release(s);
      }
    }
  }

  private com.yelp.nrtsearch.server.grpc.Query getQuery() {
    Script script = Script.newBuilder().setLang("test_lang").setSource("3.0*4.0").build();
    return com.yelp.nrtsearch.server.grpc.Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setScript(script)
                .setQuery(
                    com.yelp.nrtsearch.server.grpc.Query.newBuilder()
                        .setRangeQuery(
                            RangeQuery.newBuilder()
                                .setField("int_field")
                                .setLower(String.valueOf(0))
                                .setUpper(String.valueOf(NUM_DOCS))
                                .build())
                        .build())
                .build())
        .build();
  }

  private static List<VirtualField> getVirtualFields() {
    List<VirtualField> fields = new ArrayList<>();
    fields.add(
        VirtualField.newBuilder()
            .setName("v1")
            .setScript(Script.newBuilder().setLang("test_lang").setSource("return 2*2;").build())
            .build());
    return fields;
  }
}
