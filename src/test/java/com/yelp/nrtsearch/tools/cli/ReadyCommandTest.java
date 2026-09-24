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
package com.yelp.nrtsearch.tools.cli;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class ReadyCommandTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer server;

  private void startServer() throws IOException {
    server = TestServer.builder(folder).build();
  }

  private void createIndex(String indexName) {
    server.createIndex(indexName);
  }

  private void startIndex(String indexName) {
    server
        .getClient()
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder().setIndexName(indexName).setMode(Mode.PRIMARY).build());
  }

  @Test
  public void testNoServer() {
    CommandLine cmd = new CommandLine(new NrtsearchClientCommand());
    int exitCode = cmd.execute("--hostname=localhost", "--port=12345", "ready");
    assertEquals(1, exitCode);
  }

  @Test
  public void testNoIndices() throws IOException {
    startServer();
    CommandLine cmd = new CommandLine(new NrtsearchClientCommand());
    int exitCode = cmd.execute("--hostname=localhost", "--port=" + server.getPort(), "ready");
    assertEquals(0, exitCode);
  }

  @Test
  public void testNoIndex() throws IOException {
    startServer();
    verifyIndicesReady("test_index", 1);
  }

  @Test
  public void testIndexNotStarted() throws IOException {
    startServer();
    createIndex("test_index");
    verifyIndicesReady("test_index", 1);
  }

  @Test
  public void testIndexReady() throws IOException {
    startServer();
    createIndex("test_index");
    startIndex("test_index");
    verifyIndicesReady("test_index", 0);
  }

  @Test
  public void testMultipleIndices() throws IOException {
    startServer();
    createIndex("test_index");
    startIndex("test_index");
    createIndex("test_index_2");
    startIndex("test_index_2");
    verifyIndicesReady("test_index", 0);
    verifyIndicesReady("test_index_2", 0);
    verifyIndicesReady("test_index,test_index_2", 0);
    verifyIndicesReady("test_index,test_index_3", 1);

    CommandLine cmd = new CommandLine(new NrtsearchClientCommand());
    int exitCode = cmd.execute("--hostname=localhost", "--port=" + server.getPort(), "ready");
    assertEquals(0, exitCode);
  }

  @Test
  public void testSomeIndicesReady() throws IOException {
    startServer();
    createIndex("test_index");
    startIndex("test_index");
    createIndex("test_index_2");
    createIndex("test_index_3");
    startIndex("test_index_3");
    verifyIndicesReady("test_index", 0);
    verifyIndicesReady("test_index_2", 1);
    verifyIndicesReady("test_index_3", 0);
    verifyIndicesReady("test_index,test_index_2", 1);
    verifyIndicesReady("test_index,test_index_3", 0);

    CommandLine cmd = new CommandLine(new NrtsearchClientCommand());
    int exitCode = cmd.execute("--hostname=localhost", "--port=" + server.getPort(), "ready");
    assertEquals(0, exitCode);
  }

  private void verifyIndicesReady(String indices, int expectedExitCode) {
    CommandLine cmd = new CommandLine(new NrtsearchClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost", "--port=" + server.getPort(), "ready", "--indices=" + indices);
    assertEquals(expectedExitCode, exitCode);
  }
}
