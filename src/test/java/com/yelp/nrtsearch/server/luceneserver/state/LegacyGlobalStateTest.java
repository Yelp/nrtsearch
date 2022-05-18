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
package com.yelp.nrtsearch.server.luceneserver.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDefCreator;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import com.yelp.nrtsearch.server.luceneserver.index.LegacyIndexState;
import com.yelp.nrtsearch.server.luceneserver.index.LegacyStateManager;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LegacyGlobalStateTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    LuceneServerConfiguration dummyConfig =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    List<Plugin> dummyPlugins = Collections.emptyList();
    // these must be initialized to create an IndexState
    FieldDefCreator.initialize(dummyConfig, dummyPlugins);
    SimilarityCreator.initialize(dummyConfig, dummyPlugins);
  }

  private GlobalState getLegacyGlobalState() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LEGACY",
            "stateDir: " + folder.newFolder("state").getAbsolutePath(),
            "indexDir: " + folder.newFolder("index").getAbsolutePath());
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes()));
    return GlobalState.createState(configuration);
  }

  @Test
  public void testGetDataResourceForIndex() throws IOException {
    GlobalState globalState = getLegacyGlobalState();
    assertEquals("some_index_name", globalState.getDataResourceForIndex("some_index_name"));
  }

  @Test
  public void testGetIndexStateManager() throws IOException {
    GlobalState globalState = getLegacyGlobalState();
    globalState.createIndex("test_index");
    IndexStateManager indexStateManager = globalState.getIndexStateManager("test_index");
    assertTrue(indexStateManager instanceof LegacyStateManager);
    assertTrue(indexStateManager.getCurrent() instanceof LegacyIndexState);
  }

  @Test
  public void testGetIndicesToStart() throws IOException {
    GlobalState globalState = getLegacyGlobalState();
    globalState.createIndex("test_index");
    assertEquals(globalState.getIndexNames(), globalState.getIndicesToStart());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testStartIndex() throws IOException {
    GlobalState globalState = getLegacyGlobalState();
    globalState.startIndex(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testStopIndex() throws IOException {
    GlobalState globalState = getLegacyGlobalState();
    globalState.stopIndex(null);
  }
}
