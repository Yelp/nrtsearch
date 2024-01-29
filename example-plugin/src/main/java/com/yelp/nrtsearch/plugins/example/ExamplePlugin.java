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
package com.yelp.nrtsearch.plugins.example;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.analysis.AnalysisProvider;
import com.yelp.nrtsearch.server.plugins.AnalysisPlugin;
import com.yelp.nrtsearch.server.plugins.CustomRequestPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.util.Version;

public class ExamplePlugin extends Plugin implements AnalysisPlugin, CustomRequestPlugin {

  private final String availableAnalyzers = String.join(",", getAnalyzers().keySet());

  // Constructor that accepts LuceneServerConfiguration object is required
  public ExamplePlugin(LuceneServerConfiguration configuration) {}

  @Override
  public String id() {
    return "custom_analyzers";
  }

  @Override
  public Map<String, RequestProcessor> getRoutes() {
    return Map.of(
        "get_available_analyzers",
        (path, request) -> Map.of("available_analyzers", availableAnalyzers));
  }

  @Override
  public Map<String, AnalysisProvider<? extends Analyzer>> getAnalyzers() {
    return Map.of(
        "plugin_analyzer",
        (name) -> {
          try {
            return CustomAnalyzer.builder()
                .withDefaultMatchVersion(Version.LATEST)
                .addCharFilter("htmlstrip")
                .withTokenizer("classic")
                .addTokenFilter("lowercase")
                .build();
          } catch (Exception e) {
            return null;
          }
        });
  }
}
