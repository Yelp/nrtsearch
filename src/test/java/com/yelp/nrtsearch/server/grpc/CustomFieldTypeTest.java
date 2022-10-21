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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefProvider;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.plugins.FieldTypePlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CustomFieldTypeTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private GrpcServer grpcServer;
  private CollectorRegistry collectorRegistry;

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        luceneServerConfiguration.getPort(),
        null,
        Collections.singletonList(new TestFieldTypePlugin()));
  }

  static class TestFieldDef extends IndexableFieldDef {

    public TestFieldDef(String name, Field requestField) {
      super(name, requestField);
    }

    @Override
    protected DocValuesType parseDocValuesType(Field requestField) {
      return DocValuesType.NUMERIC;
    }

    @Override
    public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
      NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), getName());
      return new LoadedDocValues.SingleInteger(numericDocValues);
    }

    @Override
    public void parseDocumentField(
        Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
      int val = Integer.parseInt(fieldValues.get(0)) + 10;
      org.apache.lucene.document.Field field = new NumericDocValuesField(getName(), val);
      document.add(field);
    }

    @Override
    public String getType() {
      return "custom_field_type";
    }
  }

  static class TestFieldTypePlugin extends Plugin implements FieldTypePlugin {
    @Override
    public Map<String, FieldDefProvider<? extends FieldDef>> getFieldTypes() {
      Map<String, FieldDefProvider<? extends FieldDef>> typeMap = new HashMap<>();
      typeMap.put("custom_field_type", TestFieldDef::new);
      return typeMap;
    }
  }

  @Test
  public void testCustomFieldDef() throws Exception {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsCustomType.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsCustomType.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .addRetrieveFields("doc_id")
                    .addRetrieveFields("int_field")
                    .addRetrieveFields("custom_field")
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        "1", searchResponse.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        100,
        searchResponse.getHits(0).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(
        110,
        searchResponse.getHits(0).getFieldsOrThrow("custom_field").getFieldValue(0).getIntValue());
    assertEquals(
        "2", searchResponse.getHits(1).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        1001,
        searchResponse.getHits(1).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(
        1011,
        searchResponse.getHits(1).getFieldsOrThrow("custom_field").getFieldValue(0).getIntValue());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testNoTypeProperty() throws Exception {
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsCustomTypeInvalid.json");
  }
}
