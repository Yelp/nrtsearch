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
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LuceneServerDocIdTest {

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
  public void testDocIdUpdates() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsBasicWithDocId.json");
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    assertFalse(testAddDocs.error);
    assertTrue(testAddDocs.completed);

    // add 2 more docs
    testAddDocs.addDocuments();
    assertFalse(testAddDocs.error);
    assertTrue(testAddDocs.completed);

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
    testAddDocs.addDocuments("addDocsUpdated.csv");
    assertFalse(testAddDocs.error);
    assertTrue(testAddDocs.completed);
    stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are 4 documents in total
    assertEquals(4, stats.getNumDocs());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultipleDocIds() throws Exception {
    try {
      new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
      new GrpcServer.IndexAndRoleManager(grpcServer)
          .createStartIndexAndRegisterFields(
              Mode.STANDALONE, 0, false, "registerFieldsMultipleDocIds.json");
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "multiple docId fields found";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultiValued() throws Exception {
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsWithMultivaluedDocId.json");
  }

  @Test(expected = StatusRuntimeException.class)
  public void testStoreDocValuesFalse() throws Exception {
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsWithFalseDocValues.json");
  }
}
