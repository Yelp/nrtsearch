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
package com.yelp.nrtsearch.server.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.plugins.HitsLoggerPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.search.SearchContext;
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
      public final Map<String, Object> params;

      public CustomHitsLogger(Map<String, Object> params) {
        this.params = params;
      }

      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {}
    }

    static class CustomHitsLogger2 implements HitsLogger {
      public CustomHitsLogger2(Map<String, Object> params) {}

      @Override
      public void log(SearchContext context, List<SearchResponse.Hit.Builder> hits) {}
    }

    @Override
    public Map<String, HitsLoggerProvider<? extends HitsLogger>> getHitsLoggers() {
      return Map.of(
          "custom_logger", TestHitsLoggerPlugin.CustomHitsLogger::new,
          "custom_logger_2", TestHitsLoggerPlugin.CustomHitsLogger2::new);
    }
  }

  @Test()
  public void testPluginHitsLoggerNotDefined() {
    init(Collections.singletonList(new TestHitsLoggerPlugin()));

    LoggingHits loggingHits = LoggingHits.newBuilder().setName("logger_test").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HitsLoggerCreator.getInstance().createHitsLogger(loggingHits));

    String expectedMessage =
        "Unknown hits logger name [logger_test] is specified; The available hits loggers are %s";

    assertTrue(
        String.format(expectedMessage, "[custom_logger, custom_logger_2]")
                .equals(exception.getMessage())
            || String.format(expectedMessage, "[custom_logger_2, custom_logger]")
                .equals(exception.getMessage()));
  }

  @Test
  public void testPluginProvidesHitsLogger() {
    init(Collections.singletonList(new TestHitsLoggerPlugin()));
    HitsLogger hitsLogger =
        HitsLoggerCreator.getInstance()
            .createHitsLogger(LoggingHits.newBuilder().setName("custom_logger_2").build());
    assertTrue(hitsLogger instanceof TestHitsLoggerPlugin.CustomHitsLogger2);
  }

  @Test
  public void testPluginDuplicateHitsLogger() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> init(List.of(new TestHitsLoggerPlugin(), new TestHitsLoggerPlugin())));

    String expectedMessage = "HitsLogger %s already exists";
    assertTrue(
        String.format(expectedMessage, "custom_logger").equals(exception.getMessage())
            || String.format(expectedMessage, "custom_logger_2").equals(exception.getMessage()));
  }

  @Test
  public void testPluginProvidesHitsLoggerWithParams() {
    init(Collections.singletonList(new TestHitsLoggerPlugin()));

    HitsLogger hitsLogger =
        HitsLoggerCreator.getInstance()
            .createHitsLogger(
                LoggingHits.newBuilder()
                    .setName("custom_logger")
                    .setParams(
                        Struct.newBuilder()
                            .putFields(
                                "external_value", Value.newBuilder().setStringValue("abc").build()))
                    .build());
    assertTrue(hitsLogger instanceof TestHitsLoggerPlugin.CustomHitsLogger);

    TestHitsLoggerPlugin.CustomHitsLogger customHitsLogger =
        (TestHitsLoggerPlugin.CustomHitsLogger) hitsLogger;
    assertEquals(1, customHitsLogger.params.size());
    assertEquals("abc", customHitsLogger.params.get("external_value"));
  }
}
