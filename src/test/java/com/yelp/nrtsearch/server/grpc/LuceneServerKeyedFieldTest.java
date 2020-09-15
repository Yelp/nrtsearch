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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Keyable;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LuceneServerKeyedFieldTest {

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private GrpcServer grpcServer;

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
    CollectorRegistry collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    GlobalState globalState = new GlobalState(luceneServerConfiguration);
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        false,
        globalState,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        globalState.getPort(),
        null,
        Collections.emptyList());
  }

  @Test
  public void testIntKeyedFieldUpdatesAndRetrieval() throws IOException, InterruptedException {
    assertAddUpdateAndSearch("registerFieldsBasicWithIntKeyedField.json");
  }

  @Test
  public void testLongKeyedFieldUpdatesAndRetrieval() throws IOException, InterruptedException {
    assertAddUpdateAndSearch("registerFieldsBasicWithLongKeyedField.json");
  }

  @Test
  public void testAtomKeyedFieldUpdatesAndRetrieval() throws IOException, InterruptedException {
    assertAddUpdateAndSearch("registerFieldsBasicWithAtomKeyedField.json");
  }

  private void assertAddUpdateAndSearch(String registerFilePath)
      throws IOException, InterruptedException {
    GrpcServer.TestServer testServer =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, registerFilePath);
    // 2 docs addDocuments
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    // add 2 more docs
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    StatsResponse stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are only 2 documents
    assertEquals(2, stats.getNumDocs());

    // update schema: add a new field
    grpcServer
        .getBlockingStub()
        .updateFields(
            FieldDefRequest.newBuilder()
                .setIndexName("test_index")
                .addField(
                    Field.newBuilder()
                        .setName("new_text_field")
                        .setType(FieldType.TEXT)
                        .setStoreDocValues(true)
                        .setSearch(true)
                        .setMultiValued(true)
                        .setTokenize(true)
                        .build())
                .build());
    // 2 docs addDocuments
    testServer.addDocuments("addDocsUpdated.csv");
    assertFalse(testServer.error);
    assertTrue(testServer.completed);
    stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are 4 documents in total
    assertEquals(4, stats.getNumDocs());

    // Query for first document
    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("doc_id", "vendor_name"))
                    .setQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setField("vendor_name")
                                    .addTerms("first")
                                    .addTerms("vendor")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getTotalHits().getValue());
    assertEquals(1, searchResponse.getHitsList().size());
    assertEquals(
        "1",
        searchResponse.getHits(0).getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
  }

  // No exceptions when registering with storeDocValues=false, but store=true
  @Test
  public void testStoreDocValuesFalse() throws Exception {
    registerFields(List.of(getFieldBuilder("doc_id", false, true, false)));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testStoreFalse() throws Exception {
    registerFields(List.of(getFieldBuilder("doc_id", true, false, false)));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultipleDocIds() throws Exception {
    try {
      registerFields(
          List.of(
              getFieldBuilder("doc_id", true, true, false),
              getFieldBuilder("doc_id_2", true, true, false)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field \"doc_id_2\" cannot be registered as as key as \"doc_id\" is already declared as a key field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultiValued() throws Exception {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", true, true, true)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field: doc_id cannot have both multivalued and docId set to true. Only single docId is supported";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testStoreAndDocValuesFalse() throws Exception {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", false, false, false)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field: doc_id is a keyable field and should have store=true";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  // Keyable field types don't throw exceptions
  @Test
  public void testFieldTypesForDocIdsSupported() throws Exception {
    List<Field> fields = getFields(true);
    for (Field f : fields) {
      Field field = getFieldBuilder("doc_id", false, true, false, f.getType());
      registerFields(field.getType().toString(), List.of(field));
    }
  }

  // Non Keyable field types throw exceptions
  @Test(expected = StatusRuntimeException.class)
  public void testFieldTypesDocIdsNotSupported() throws Exception {
    List<Exception> exceptions = new LinkedList<>();
    List<Field> fields = getFields(false);
    for (Field f : fields) {
      Field field = getFieldBuilder("doc_id", true, false, false, f.getType());
      String suffix = field.getType().toString();
      try {
        registerFields(suffix, List.of(field));
      } catch (RuntimeException e) {
        String message =
            "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index"
                + suffix
                + "\n"
                + "field: \"doc_id\" is not a keyable field type";
        assertEquals(message, e.getMessage());
        exceptions.add(e);
      }
    }
    assertEquals(fields.size(), exceptions.size());
    throw (exceptions.get(0));
  }

  private List<Field> getFields(boolean keyable) {
    List<Field> fields = new LinkedList<>();
    for (FieldType fieldType : FieldType.values()) {
      if (fieldType == FieldType.UNRECOGNIZED || fieldType.getNumber() > 8) {
        continue;
      }
      Field field = Field.newBuilder().setType(fieldType).build();
      FieldDef fieldDef = new FieldDefCreator(null).createFieldDef("", fieldType.name(), field);
      if (keyable == fieldDef instanceof Keyable) {
        fields.add(field);
      }
    }
    return fields;
  }

  private Field getFieldBuilder(
      String fieldName, boolean storeDocValues, boolean store, boolean multiValued) {
    return getFieldBuilder(fieldName, storeDocValues, store, multiValued, FieldType.ATOM);
  }

  private Field getFieldBuilder(
      String fieldName,
      boolean storeDocValues,
      boolean store,
      boolean multiValued,
      FieldType fieldType) {
    return Field.newBuilder()
        .setName(fieldName)
        .setKey(true)
        .setStoreDocValues(storeDocValues)
        .setStore(store)
        .setType(fieldType)
        .setMultiValued(multiValued)
        .build();
  }

  private void registerFields(List<Field> fields) throws IOException {
    registerFields("", fields);
  }

  private void registerFields(String suffix, List<Field> fields) throws IOException {
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    String indexName = grpcServer.getTestIndex() + suffix;
    grpcServer
        .getBlockingStub()
        .createIndex(
            CreateIndexRequest.newBuilder()
                .setIndexName(indexName)
                .setRootDir(grpcServer.getIndexDir())
                .build());
    FieldDefRequest.Builder builder = FieldDefRequest.newBuilder().setIndexName(indexName);
    for (Field field : fields) {
      builder.addField(field);
    }
    grpcServer.getBlockingStub().registerFields(builder.build());
  }
}
