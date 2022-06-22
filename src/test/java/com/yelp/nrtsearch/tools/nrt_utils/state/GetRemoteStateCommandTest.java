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
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.index.ImmutableIndexState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class GetRemoteStateCommandTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private AmazonS3 getS3() {
    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    return s3;
  }

  private CommandLine getInjectedCommand() {
    GetRemoteStateCommand command = new GetRemoteStateCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    return server;
  }

  @Test
  public void testGetGlobalStateNotExists() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    File stateFile = Paths.get(folder.getRoot().getPath(), "state_file.json").toFile();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--outputFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);
    assertFalse(stateFile.exists());
  }

  @Test
  public void testGetIndexStateNotExists() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();
    File stateFile = Paths.get(folder.getRoot().getPath(), "state_file.json").toFile();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index",
            "--outputFile=" + stateFile.getAbsolutePath(),
            "--exactResourceName");
    assertEquals(0, exitCode);
    assertFalse(stateFile.exists());
  }

  @Test
  public void testGetGlobalState() throws IOException {
    getTestServer();
    CommandLine cmd = getInjectedCommand();
    File stateFile = Paths.get(folder.getRoot().getPath(), "state_file.json").toFile();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--outputFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);
    assertTrue(stateFile.exists());

    String contents = StateUtils.fromUTF8(Files.readAllBytes(stateFile.toPath()));
    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    GlobalStateInfo stateInfo = builder.build();
    assertEquals(1, stateInfo.getIndicesMap().size());
    assertTrue(stateInfo.getIndicesMap().containsKey("test_index"));
    assertFalse(stateInfo.getIndicesMap().get("test_index").getStarted());
  }

  @Test
  public void testGetIndexState() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedCommand();
    File stateFile = Paths.get(folder.getRoot().getPath(), "state_file.json").toFile();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index",
            "--outputFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);
    assertTrue(stateFile.exists());

    String contents = StateUtils.fromUTF8(Files.readAllBytes(stateFile.toPath()));
    IndexStateInfo.Builder builder = IndexStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    IndexStateInfo stateInfo = builder.build();
    IndexStateInfo expected =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(expected, stateInfo);
  }

  @Test
  public void testGetIndexStateExact() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedCommand();
    File stateFile = Paths.get(folder.getRoot().getPath(), "state_file.json").toFile();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + server.getGlobalState().getDataResourceForIndex("test_index"),
            "--exactResourceName",
            "--outputFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);
    assertTrue(stateFile.exists());

    String contents = StateUtils.fromUTF8(Files.readAllBytes(stateFile.toPath()));
    IndexStateInfo.Builder builder = IndexStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    IndexStateInfo stateInfo = builder.build();
    IndexStateInfo expected =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(expected, stateInfo);
  }
}
