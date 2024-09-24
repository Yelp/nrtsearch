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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class ListResourceVersionsTest {
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
    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    return s3;
  }

  private CommandLine getInjectedCommand() {
    ListResourceVersions command = new ListResourceVersions();
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
  public void testListResourceVersions() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.INDEX_STATE);
    AmazonS3 s3 = getS3();
    String version1 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version1, "test");
    String version2 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version2, "test");
    String version3 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version3, "test");

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    String output = out.toString();
    assertTrue(output.contains(version1 + " ("));
    assertTrue(output.contains(version2 + " ("));
    assertTrue(output.contains(version3 + " ("));
  }

  @Test
  public void testListResourceVersions_versionPrefix() throws IOException, InterruptedException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.INDEX_STATE);
    AmazonS3 s3 = getS3();
    String version1 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version1, "test");
    Thread.sleep(2000);
    String version2 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version2, "test");
    Thread.sleep(2000);
    String version3 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version3, "test");
    String versionPrefix = version2.split("-")[0];

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INDEX_STATE",
            "--versionPrefix=" + versionPrefix);
    assertEquals(0, exitCode);
    String output = out.toString();
    assertFalse(output.contains(version1 + " ("));
    assertTrue(output.contains(version2 + " ("));
    assertFalse(output.contains(version3 + " ("));
  }

  @Test
  public void testListResourceVersions_unexpectedFormat() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, "test_index-id", RemoteBackend.IndexResourceType.INDEX_STATE);
    AmazonS3 s3 = getS3();
    String version1 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version1, "test");
    String version2 = "unexpected";
    s3.putObject(TEST_BUCKET, prefix + version2, "test");
    String version3 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version3, "test");

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index-id",
            "--exactResourceName",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    String output = out.toString();
    assertTrue(output.contains(version1 + " ("));
    assertTrue(output.contains(version2));
    assertTrue(output.contains(version3 + " ("));
  }

  @Test
  public void testListResourceNoVersions() throws IOException {
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
    assertTrue(
        out.toString()
            .contains(
                "Listing versions for test_index-id with prefix test_server/test_index-id/state/ (max 1000)"));
  }

  @Test
  public void testListResourceVersionsFromGlobalState() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    S3Backend backend = new S3Backend(TEST_BUCKET, false, getS3());
    String indexId = server.getGlobalState().getIndexStateManager("test_index").getIndexId();
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME,
            BackendGlobalState.getUniqueIndexName("test_index", indexId),
            RemoteBackend.IndexResourceType.INDEX_STATE);
    String currentVersion = backend.getCurrentResourceName(prefix);
    AmazonS3 s3 = getS3();
    String version1 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version1, "test");
    String version2 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version2, "test");
    String version3 = S3Backend.getIndexStateFileName();
    s3.putObject(TEST_BUCKET, prefix + version3, "test");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index",
            "--indexResourceType=INDEX_STATE");
    assertEquals(0, exitCode);
    String output = out.toString();
    assertTrue(output.contains(currentVersion + " ("));
    assertTrue(output.contains(version1 + " ("));
    assertTrue(output.contains(version2 + " ("));
    assertTrue(output.contains(version3 + " ("));
  }
}
