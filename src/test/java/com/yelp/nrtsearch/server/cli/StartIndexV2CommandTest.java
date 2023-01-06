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
package com.yelp.nrtsearch.server.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class StartIndexV2CommandTest {
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
  public void testStartIndexV2() throws IOException {
    TestServer server = getTestServer();
    server.createIndex("test_index");

    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "startIndexV2",
            "--indexName=test_index");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));
  }
}
