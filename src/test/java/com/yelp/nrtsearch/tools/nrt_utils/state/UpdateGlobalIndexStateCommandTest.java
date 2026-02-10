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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class UpdateGlobalIndexStateCommandTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private S3Client getS3() {
    S3Client s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    s3.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
    return s3;
  }

  private CommandLine getInjectedCommand() {
    UpdateGlobalIndexStateCommand command = new UpdateGlobalIndexStateCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    return server;
  }

  @Test
  public void testUpdateIndexStarted() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    assertTrue(server.isStarted("test_index"));

    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index",
            "--setStarted=false");
    assertEquals(0, exitCode);
    server.restart();
    assertFalse(server.isStarted("test_index"));

    exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index",
            "--setStarted=true");
    assertEquals(0, exitCode);
    server.restart();
    assertTrue(server.isStarted("test_index"));
  }

  @Test
  public void testUpdateIndexUUID() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    String firstIndexId =
        server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();
    server.deleteIndex("test_index");

    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3, 4, 5);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 5);
    String secondIndexId =
        server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();
    assertNotEquals(firstIndexId, secondIndexId);

    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index",
            "--setId=" + firstIndexId);
    assertEquals(0, exitCode);
    server.restart();
    assertTrue(server.isStarted("test_index"));

    String thirdIndexId =
        server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();
    assertEquals(firstIndexId, thirdIndexId);
    server.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testNoopUpdate() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    server.createSimpleIndex("test_index_2");
    String indexId = server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();
    String index2Id =
        server.getGlobalState().getIndexStateManagerOrThrow("test_index_2").getIndexId();

    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index");
    assertEquals(0, exitCode);

    exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index_2");
    assertEquals(0, exitCode);

    server.restart();
    assertTrue(server.isStarted("test_index"));
    assertFalse(server.isStarted("test_index_2"));
    assertEquals(
        indexId, server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId());
    assertEquals(
        index2Id, server.getGlobalState().getIndexStateManagerOrThrow("test_index_2").getIndexId());
  }

  @Test
  public void testNoGlobalState() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=test_index");
    assertEquals(1, exitCode);
  }

  @Test
  public void testIndexNotInState() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);

    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=not_index");
    assertEquals(1, exitCode);
  }

  @Test
  public void testIndexIDNotInBackend() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    String indexId = server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();

    CommandLine cmd = getInjectedCommand();
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--indexName=not_index",
            "--setId=20240820123456789");
    assertEquals(1, exitCode);
    server.restart();

    assertTrue(server.isStarted("test_index"));
    assertEquals(
        indexId, server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId());
  }

  @Test
  public void testValidateStarted() {
    assertTrue(UpdateGlobalIndexStateCommand.validateParams(null, null));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams("true", null));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams("True", null));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams("false", null));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams("false", null));
    assertFalse(UpdateGlobalIndexStateCommand.validateParams("", null));
    assertFalse(UpdateGlobalIndexStateCommand.validateParams("invalid", null));
  }

  @Test
  public void testValidateIndexID() {
    assertTrue(UpdateGlobalIndexStateCommand.validateParams(null, null));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams(null, "20240820123456789"));
    assertTrue(UpdateGlobalIndexStateCommand.validateParams(null, "19701010000000000"));
    assertFalse(UpdateGlobalIndexStateCommand.validateParams(null, ""));
    assertFalse(UpdateGlobalIndexStateCommand.validateParams(null, "invalid"));
    assertFalse(UpdateGlobalIndexStateCommand.validateParams(null, "20241329233759999"));
  }
}
