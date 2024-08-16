/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.logging;

import static org.junit.Assert.*;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class HitsLoggerCreatorTest {
  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    HitsLoggerCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  public static class TestHitsLoggerPlugin extends Plugin implements HitsLoggerPlugin {
    static class CustomHitsLogger implements HitsLogger {
      public CustomHitsLogger(Map<String, Object> params) {}

      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {}
    }

    @Override
    public HitsLoggerProvider<? extends HitsLogger> getHitsLogger() {
      return CustomHitsLogger::new;
    }
  }

  @Test()
  public void testPluginHitsLoggerNotDefined() {
    LoggingHits loggingHits = LoggingHits.newBuilder().build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HitsLoggerCreator.getInstance().createHitsLogger(loggingHits));

    assertEquals("No hits logger was assigned", exception.getMessage());
  }

  @Test
  public void testPluginProvidesHitsLoggerWithParams() {
    init(Collections.singletonList(new TestHitsLoggerPlugin()));

    LoggingHits loggingHits =
        LoggingHits.newBuilder()
            .setParams(
                Struct.newBuilder()
                    .putFields("external_value", Value.newBuilder().setStringValue("abc").build()))
            .build();
    HitsLogger hitsLogger = HitsLoggerCreator.getInstance().createHitsLogger(loggingHits);

    assertTrue(hitsLogger instanceof TestHitsLoggerPlugin.CustomHitsLogger);
  }

  @Test
  public void testPluginProvidesHitsLoggerWithNoParams() {
    init(Collections.singletonList(new TestHitsLoggerPlugin()));

    LoggingHits loggingHits = LoggingHits.newBuilder().build();
    HitsLogger hitsLogger = HitsLoggerCreator.getInstance().createHitsLogger(loggingHits);

    assertTrue(hitsLogger instanceof TestHitsLoggerPlugin.CustomHitsLogger);
  }
}
