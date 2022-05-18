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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.protobuf.Int32Value;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
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

public class PutRemoteStateCommandTest {
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
    PutRemoteStateCommand command = new PutRemoteStateCommand();
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

  private VersionManager getVersionManager() {
    return new VersionManager(getS3(), TEST_BUCKET);
  }

  @Test
  public void testPutGlobalState() throws IOException {
    TestServer server = getTestServer();
    VersionManager versionManager = getVersionManager();
    String contents =
        StateCommandUtils.getStateFileContents(
            versionManager,
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertEquals(1, server.indices().size());

    CommandLine cmd = getInjectedCommand();

    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    builder.removeIndices("test_index");
    GlobalStateInfo stateInfo = builder.build();

    File stateFile = folder.newFile("new_state.json");
    StateCommandUtils.writeStringToFile(JsonFormat.printer().print(stateInfo), stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--stateFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);

    server.restart();
    assertEquals(0, server.indices().size());
  }

  @Test
  public void testPutIndexState() throws IOException {
    TestServer server = getTestServer();
    IndexStateInfo currentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    IndexStateInfo updatedState =
        currentState
            .toBuilder()
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setSliceMaxSegments(Int32Value.newBuilder().setValue(1).build())
                    .build())
            .build();

    CommandLine cmd = getInjectedCommand();

    File stateFile = folder.newFile("new_state.json");
    StateCommandUtils.writeStringToFile(JsonFormat.printer().print(updatedState), stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=test_index",
            "--stateFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);

    server.restart();
    IndexStateInfo newCurrentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(updatedState, newCurrentState);
    assertNotEquals(currentState, newCurrentState);
  }

  @Test
  public void testPutIndexStateExact() throws IOException {
    TestServer server = getTestServer();
    IndexStateInfo currentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    IndexStateInfo updatedState =
        currentState
            .toBuilder()
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setSliceMaxSegments(Int32Value.newBuilder().setValue(1).build())
                    .build())
            .build();

    CommandLine cmd = getInjectedCommand();

    File stateFile = folder.newFile("new_state.json");
    StateCommandUtils.writeStringToFile(JsonFormat.printer().print(updatedState), stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + server.getGlobalState().getDataResourceForIndex("test_index"),
            "--exactResourceName",
            "--stateFile=" + stateFile.getAbsolutePath());
    assertEquals(0, exitCode);

    server.restart();
    IndexStateInfo newCurrentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(updatedState, newCurrentState);
    assertNotEquals(currentState, newCurrentState);
  }

  @Test
  public void testInvalidState() throws IOException {
    TestServer.initS3(folder);
    String stateStr =
        "{\"ind\":{\"test_index\":{\"id\":\"09d9c9e4-483e-4a90-9c4f-d342c8da1210\",\"started\":true}}}";
    CommandLine cmd = getInjectedCommand();

    File stateFile = folder.newFile("new_state.json");
    StateCommandUtils.writeStringToFile(stateStr, stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--stateFile=" + stateFile.getAbsolutePath());
    assertEquals(1, exitCode);

    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertNull(contents);
  }

  @Test
  public void testSkipValidate() throws IOException {
    TestServer.initS3(folder);
    String stateStr =
        "{\"ind\":{\"test_index\":{\"id\":\"09d9c9e4-483e-4a90-9c4f-d342c8da1210\",\"started\":true}}}";
    CommandLine cmd = getInjectedCommand();

    File stateFile = folder.newFile("new_state.json");
    StateCommandUtils.writeStringToFile(stateStr, stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--stateFile=" + stateFile.getAbsolutePath(),
            "--skipValidate");
    assertEquals(0, exitCode);

    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertNotNull(contents);
  }

  @Test
  public void testBackupStateNotPresent() throws IOException {
    TestServer.initS3(folder);
    CommandLine cmd = getInjectedCommand();

    GlobalStateInfo stateInfo = GlobalStateInfo.newBuilder().build();

    File stateFile = folder.newFile("new_state.json");
    File backupFile = Paths.get(folder.getRoot().getPath(), "backup.json").toFile();
    StateCommandUtils.writeStringToFile(JsonFormat.printer().print(stateInfo), stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--stateFile=" + stateFile.getAbsolutePath(),
            "--backupFile=" + backupFile.getAbsolutePath());
    assertEquals(0, exitCode);
    assertFalse(backupFile.exists());
  }

  @Test
  public void testBackupFile() throws IOException {
    TestServer server = getTestServer();
    VersionManager versionManager = getVersionManager();
    String contents =
        StateCommandUtils.getStateFileContents(
            versionManager,
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertEquals(1, server.indices().size());

    CommandLine cmd = getInjectedCommand();

    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    builder.removeIndices("test_index");
    GlobalStateInfo stateInfo = builder.build();

    File stateFile = folder.newFile("new_state.json");
    File backupFile = Paths.get(folder.getRoot().getPath(), "backup.json").toFile();
    StateCommandUtils.writeStringToFile(JsonFormat.printer().print(stateInfo), stateFile);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--bucketName=" + TEST_BUCKET,
            "--resourceName=" + RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "--stateFile=" + stateFile.getAbsolutePath(),
            "--backupFile=" + backupFile.getAbsolutePath());
    assertEquals(0, exitCode);

    GlobalStateInfo.Builder expectedStateBuilder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, expectedStateBuilder);
    GlobalStateInfo expectedState = expectedStateBuilder.build();

    assertTrue(backupFile.exists());
    byte[] backupFileBytes = Files.readAllBytes(backupFile.toPath());
    GlobalStateInfo.Builder backupStateBuilder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(StateUtils.fromUTF8(backupFileBytes), backupStateBuilder);
    GlobalStateInfo backupState = backupStateBuilder.build();
    assertEquals(expectedState, backupState);
  }
}
