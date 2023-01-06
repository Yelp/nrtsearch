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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FloatFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.LongFieldDef;
import com.yelp.nrtsearch.server.luceneserver.index.ImmutableIndexState;
import java.io.IOException;
import java.util.Map;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class CreateIndexCommandTest {
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
  public void testCreateIndex() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    assertEquals(ImmutableIndexState.DEFAULT_INDEX_SETTINGS, getIndexSettings(server));
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, getIndexLiveSettings(server));
    assertTrue(getIndexFields(server).isEmpty());
  }

  @Test
  public void testCreateIndexWithSettings() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index",
            "--settings={\"directory\": \"SimpleFSDirectory\"}");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    IndexSettings settings = getIndexSettings(server);
    assertEquals("SimpleFSDirectory", settings.getDirectory().getValue());

    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, getIndexLiveSettings(server));
    assertTrue(getIndexFields(server).isEmpty());
  }

  @Test
  public void testCreateIndexWithLiveSettings() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index",
            "--liveSettings={\"maxSearcherAgeSec\": 111}");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    IndexLiveSettings settings = getIndexLiveSettings(server);
    assertEquals(111.0, settings.getMaxSearcherAgeSec().getValue(), 0);

    assertEquals(ImmutableIndexState.DEFAULT_INDEX_SETTINGS, getIndexSettings(server));
    assertTrue(getIndexFields(server).isEmpty());
  }

  @Test
  public void testCreateIndexWithFields() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index",
            "--fields={\"field\": [{\"name\": \"field1\", \"type\": \"LONG\", \"search\": true}, {\"name\": \"field2\", \"type\": \"FLOAT\", \"storeDocValues\": true}]}");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    Map<String, FieldDef> fields = getIndexFields(server);
    assertEquals(2, fields.size());
    FieldDef fieldDef = fields.get("field1");
    assertNotNull(fieldDef);
    assertTrue(fieldDef instanceof LongFieldDef);
    assertTrue(((LongFieldDef) fieldDef).isSearchable());

    fieldDef = fields.get("field2");
    assertNotNull(fieldDef);
    assertTrue(fieldDef instanceof FloatFieldDef);
    assertTrue(((FloatFieldDef) fieldDef).hasDocValues());

    assertEquals(ImmutableIndexState.DEFAULT_INDEX_SETTINGS, getIndexSettings(server));
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, getIndexLiveSettings(server));
  }

  @Test
  public void testCreateIndexWithAll() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index",
            "--settings={\"directory\": \"SimpleFSDirectory\"}",
            "--liveSettings={\"maxSearcherAgeSec\": 111}",
            "--fields={\"field\": [{\"name\": \"field1\", \"type\": \"LONG\", \"search\": true}, {\"name\": \"field2\", \"type\": \"FLOAT\", \"storeDocValues\": true}]}");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    IndexSettings settings = getIndexSettings(server);
    assertEquals("SimpleFSDirectory", settings.getDirectory().getValue());

    IndexLiveSettings liveSettings = getIndexLiveSettings(server);
    assertEquals(111.0, liveSettings.getMaxSearcherAgeSec().getValue(), 0);

    Map<String, FieldDef> fields = getIndexFields(server);
    assertEquals(2, fields.size());
    FieldDef fieldDef = fields.get("field1");
    assertNotNull(fieldDef);
    assertTrue(fieldDef instanceof LongFieldDef);
    assertTrue(((LongFieldDef) fieldDef).isSearchable());

    fieldDef = fields.get("field2");
    assertNotNull(fieldDef);
    assertTrue(fieldDef instanceof FloatFieldDef);
    assertTrue(((FloatFieldDef) fieldDef).hasDocValues());
  }

  @Test
  public void testCreateAndStartIndex() throws IOException {
    TestServer server = getTestServer();

    CommandLine cmd = new CommandLine(new LuceneClientCommand());
    int exitCode =
        cmd.execute(
            "--hostname=localhost",
            "--port=" + server.getPort(),
            "createIndex",
            "--indexName=test_index",
            "--settings={\"directory\": \"SimpleFSDirectory\"}",
            "--start");
    assertEquals(0, exitCode);
    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));

    IndexSettings settings = getIndexSettings(server);
    assertEquals("SimpleFSDirectory", settings.getDirectory().getValue());

    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, getIndexLiveSettings(server));
    assertTrue(getIndexFields(server).isEmpty());
  }

  private IndexSettings getIndexSettings(TestServer server) throws IOException {
    return server.getGlobalState().getIndexStateManager("test_index").getSettings();
  }

  private IndexLiveSettings getIndexLiveSettings(TestServer server) throws IOException {
    return server.getGlobalState().getIndexStateManager("test_index").getLiveSettings();
  }

  private Map<String, FieldDef> getIndexFields(TestServer server) throws IOException {
    return server.getGlobalState().getIndex("test_index").getAllFields();
  }
}
