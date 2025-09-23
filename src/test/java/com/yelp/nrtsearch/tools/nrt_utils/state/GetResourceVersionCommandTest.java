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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class GetResourceVersionCommandTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  final PrintStream originalOut = System.out;
  final PrintStream originalErr = System.err;
  final ByteArrayOutputStream out = new ByteArrayOutputStream();
  final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @Before
  public void setUpStreams() {
    out.reset();
    err.reset();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @After
  public void cleanup() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    TestServer.cleanupAll();
  }

  private AmazonS3 getS3() {
    AmazonS3 s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    return s3;
  }

  private CommandLine getInjectedCommand() {
    GetResourceVersionCommand command = new GetResourceVersionCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.LOCAL)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    return server;
  }

  @Test
  public void testNotSet_globalState() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + StateCommandUtils.GLOBAL_STATE_RESOURCE);
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: not_set"));
  }

  @Test
  public void testNotSet_indexState() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: not_set"));
  }

  @Test
  public void testNotSet_pointState() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=POINT_STATE");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: not_set"));
  }

  @Test
  public void testNotSet_warmingQueries() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=WARMING_QUERIES");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: not_set"));
  }

  @Test
  public void testSet_globalState() throws IOException {
    TestServer.initS3(folder);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String prefix = S3Backend.getGlobalStateResourcePrefix(SERVICE_NAME);
    backend.setCurrentResource(prefix, "version1");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + StateCommandUtils.GLOBAL_STATE_RESOURCE);
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: version1"));
  }

  @Test
  public void testSet_indexState() throws IOException {
    TestServer.initS3(folder);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.INDEX_STATE);
    backend.setCurrentResource(prefix, "version1");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: version1"));
  }

  @Test
  public void testSet_pointState() throws IOException {
    TestServer.initS3(folder);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.POINT_STATE);
    backend.setCurrentResource(prefix, "version1");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=POINT_STATE");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: version1"));
  }

  @Test
  public void testSet_warmingQueries() throws IOException {
    TestServer.initS3(folder);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.WARMING_QUERIES);
    backend.setCurrentResource(prefix, "version1");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=WARMING_QUERIES");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: version1"));
  }

  @Test
  public void testGetResourceFromGlobalState() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String indexId = server.getGlobalState().getIndexStateManagerOrThrow("test_index").getIndexId();
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME,
            BackendGlobalState.getUniqueIndexName("test_index", indexId),
            RemoteBackend.IndexResourceType.INDEX_STATE);
    String expectedVersion = backend.getCurrentResourceName(prefix);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    assertTrue(out.toString().contains("Version: " + expectedVersion));
  }

  @Test
  public void testGetResourceFromGlobalState_notFound() throws IOException {
    TestServer.initS3(folder);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, false, getS3());
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.INDEX_STATE);
    backend.setCurrentResource(prefix, "version1");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--indexResourceType=INDEX_STATE");
    assertEquals(1, exitCode);
    assertTrue(
        err.toString()
            .startsWith(
                "java.lang.IllegalArgumentException: Unable to load global state for cluster: \"test_server\""));
  }

  @Test
  public void testInvalidIndexResourceType() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INVALID");
    assertEquals(1, exitCode);
    assertTrue(
        err.toString()
            .startsWith(
                "java.lang.IllegalArgumentException: Invalid index resource type: INVALID"));
  }
}
