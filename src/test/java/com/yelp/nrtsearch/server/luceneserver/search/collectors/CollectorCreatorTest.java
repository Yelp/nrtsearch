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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.PluginCollector;
import com.yelp.nrtsearch.server.plugins.CollectorPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.junit.Before;
import org.junit.Test;

public class CollectorCreatorTest {

  private CollectorCreatorContext collectorCreatorContext;
  private static final String COLLECTOR_NAME = "aggregated_docs";
  private static final String PLUGIN_COLLECTOR = "plugin_collector";

  @Before
  public void init() {
    init(Collections.emptyList());
    collectorCreatorContext = mock(CollectorCreatorContext.class);
  }

  private void init(List<Plugin> plugins) {
    CollectorCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  public static class TestCollectorPlugin extends Plugin implements CollectorPlugin {

    @Override
    public Map<
            String,
            CollectorProvider<
                ? extends
                    AdditionalCollectorManager<
                        ? extends org.apache.lucene.search.Collector, CollectorResult>>>
        getCollectors() {
      Map<
              String,
              CollectorProvider<
                  ? extends
                      AdditionalCollectorManager<
                          ? extends org.apache.lucene.search.Collector, CollectorResult>>>
          collectorProviderMap = new HashMap<>();
      collectorProviderMap.put(PLUGIN_COLLECTOR, CustomCollectorManager::new);
      return collectorProviderMap;
    }

    public static class CustomCollectorManager
        implements AdditionalCollectorManager<
            CustomCollectorManager.CustomCollector, CollectorResult> {
      public final String name;
      public final CollectorCreatorContext context;
      public final Map<String, Object> params;

      public CustomCollectorManager(
          String name,
          CollectorCreatorContext context,
          Map<String, Object> params,
          Map<
                  String,
                  Supplier<
                      AdditionalCollectorManager<
                          ? extends org.apache.lucene.search.Collector, CollectorResult>>>
              nestedCollectorSuppliers) {
        this.name = name;
        this.context = context;
        this.params = params;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public CustomCollector newCollector() throws IOException {
        return new CustomCollector();
      }

      @Override
      public CollectorResult reduce(Collection<CustomCollector> collectors) throws IOException {
        return null;
      }

      public static class CustomCollector implements org.apache.lucene.search.Collector {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
          return null;
        }

        @Override
        public ScoreMode scoreMode() {
          return null;
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPluginCollectorNotDefined() {
    Collector grpcCollector =
        Collector.newBuilder()
            .setPluginCollector(PluginCollector.newBuilder().setName("invalid_collector").build())
            .build();
    CollectorCreator.getInstance()
        .createCollectorManager(collectorCreatorContext, COLLECTOR_NAME, grpcCollector);
  }

  @Test
  public void testPluginProvidesCollector() {
    init(Collections.singletonList(new TestCollectorPlugin()));
    Collector grpcCollector =
        Collector.newBuilder()
            .setPluginCollector(PluginCollector.newBuilder().setName(PLUGIN_COLLECTOR).build())
            .build();
    AdditionalCollectorManager<? extends org.apache.lucene.search.Collector, CollectorResult>
        collectorManager =
            CollectorCreator.getInstance()
                .createCollectorManager(collectorCreatorContext, COLLECTOR_NAME, grpcCollector);

    assertTrue(collectorManager instanceof TestCollectorPlugin.CustomCollectorManager);
  }

  @Test
  public void testPluginCollectorParams() {
    init(Collections.singletonList(new TestCollectorPlugin()));
    Collector grpcCollector =
        Collector.newBuilder()
            .setPluginCollector(
                PluginCollector.newBuilder()
                    .setName(PLUGIN_COLLECTOR)
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "number_param", Value.newBuilder().setNumberValue(100).build())
                            .putFields(
                                "text_param", Value.newBuilder().setStringValue("param2").build())
                            .build())
                    .build())
            .build();
    AdditionalCollectorManager<? extends org.apache.lucene.search.Collector, CollectorResult>
        collectorManager =
            CollectorCreator.getInstance()
                .createCollectorManager(collectorCreatorContext, COLLECTOR_NAME, grpcCollector);
    assertTrue(collectorManager instanceof TestCollectorPlugin.CustomCollectorManager);

    TestCollectorPlugin.CustomCollectorManager testCollectorManager =
        (TestCollectorPlugin.CustomCollectorManager) collectorManager;
    assertEquals(2, testCollectorManager.params.size());
    assertEquals(100.0, testCollectorManager.params.get("number_param"));
    assertEquals("param2", testCollectorManager.params.get("text_param"));
    assertEquals(collectorCreatorContext, testCollectorManager.context);
    assertEquals(COLLECTOR_NAME, testCollectorManager.name);
  }
}
