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
package com.yelp.nrtsearch.server.luceneserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GlobalStateTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private LuceneServerConfiguration getConfig(String config) {
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testCreateBackendGlobalState() throws IOException {
    String configFile =
        String.join(
            "\n",
            "stateConfig:",
            "  backendType: LOCAL",
            "stateDir: " + folder.getRoot().getAbsolutePath());
    LuceneServerConfiguration configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration);
    assertTrue(globalState instanceof BackendGlobalState);
  }

  @Test
  public void testGetGeneration() throws IOException {
    String configFile = String.join("\n", "stateConfig:", "  backendType: LOCAL");
    LuceneServerConfiguration configuration = getConfig(configFile);
    GlobalState globalState = GlobalState.createState(configuration);
    long gen = globalState.getGeneration();
    assertTrue(gen > 0);
    assertEquals(gen, globalState.getGeneration());

    try {
      Thread.sleep(50);
    } catch (InterruptedException ignore) {
    }
    GlobalState globalState2 = GlobalState.createState(configuration);
    assertTrue(globalState2.getGeneration() > gen);
  }
}
