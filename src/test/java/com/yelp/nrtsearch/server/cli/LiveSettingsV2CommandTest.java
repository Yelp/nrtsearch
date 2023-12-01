/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.StartIndexV2Request;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class LiveSettingsV2CommandTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer getTestServer() throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
        .withLocalStateBackend()
        .build();
  }

  @Test
  public void testVerboseMetrics() throws IOException {
    TestServer server = getTestServer();
    server.createSimpleIndex("test_index");
    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertFalse(server.getGlobalState().getIndex("test_index").getVerboseMetrics());

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index");
    assertEquals(0, exitCode);
    assertFalse(server.getGlobalState().getIndex("test_index").getVerboseMetrics());

    exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index",
            "--verboseMetrics=true");
    assertEquals(0, exitCode);
    assertTrue(server.getGlobalState().getIndex("test_index").getVerboseMetrics());

    exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index");
    assertEquals(0, exitCode);
    assertTrue(server.getGlobalState().getIndex("test_index").getVerboseMetrics());

    exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index",
            "--verboseMetrics=false");
    assertEquals(0, exitCode);
    assertFalse(server.getGlobalState().getIndex("test_index").getVerboseMetrics());
  }

  @Test
  public void testVerboseMetrics_invalid() throws IOException {
    TestServer server = getTestServer();
    server.createSimpleIndex("test_index");
    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index",
            "--verboseMetrics=invalid");
    assertEquals(1, exitCode);
  }

  @Test
  public void testSetsSettings() throws IOException {
    TestServer server = getTestServer();
    server.createSimpleIndex("test_index");
    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertEquals(
        0.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index",
            "--defaultSearchTimeoutSec=1.0");
    assertEquals(0, exitCode);
    assertEquals(
        1.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);

    server.restart();

    assertEquals(
        1.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);
  }

  @Test
  public void testSetsLocalSettings() throws IOException {
    TestServer server = getTestServer();
    server.createSimpleIndex("test_index");
    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertEquals(
        0.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "liveSettingsV2",
            "--indexName=test_index",
            "--defaultSearchTimeoutSec=1.0",
            "--local");
    assertEquals(0, exitCode);
    assertEquals(
        1.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);

    server.restart();

    assertEquals(
        0.0, server.getGlobalState().getIndex("test_index").getDefaultSearchTimeoutSec(), 0);
  }
}
