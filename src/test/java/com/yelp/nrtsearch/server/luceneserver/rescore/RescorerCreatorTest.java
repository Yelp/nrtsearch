/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.rescore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.RescorerPlugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.TopDocs;
import org.junit.Before;
import org.junit.Test;

public class RescorerCreatorTest {

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    RescorerCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  public static class TestRescorerPlugin extends Plugin implements RescorerPlugin {

    @Override
    public Map<String, RescorerProvider<? extends Rescorer>> getRescorers() {
      Map<String, RescorerProvider<? extends Rescorer>> rescorerProviderMap = new HashMap<>();
      rescorerProviderMap.put("plugin_rescorer", CustomRescorer::new);
      return rescorerProviderMap;
    }

    public static class CustomRescorer extends Rescorer {

      public final Map<String, Object> params;

      public CustomRescorer(Map<String, Object> params) {
        this.params = params;
      }

      @Override
      public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN)
          throws IOException {
        return null;
      }

      @Override
      public Explanation explain(
          IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
        return null;
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPluginRescorerNotDefined() {
    RescorerCreator.getInstance().createRescorer("plugin_rescorer", Collections.emptyMap());
  }

  @Test
  public void testPluginProvidesRescorer() {
    init(Collections.singletonList(new TestRescorerPlugin()));
    Rescorer rescorer =
        RescorerCreator.getInstance().createRescorer("plugin_rescorer", Collections.emptyMap());
    assertTrue(rescorer instanceof TestRescorerPlugin.CustomRescorer);
  }

  @Test
  public void testRescorerParams() {
    init(Collections.singletonList(new TestRescorerPlugin()));
    Map<String, Object> params = new HashMap<>();
    params.put("p1", 100);
    params.put("p2", "param2");
    Rescorer rescorer = RescorerCreator.getInstance().createRescorer("plugin_rescorer", params);
    assertTrue(rescorer instanceof TestRescorerPlugin.CustomRescorer);
    assertEquals(params, ((TestRescorerPlugin.CustomRescorer) rescorer).params);
  }
}
