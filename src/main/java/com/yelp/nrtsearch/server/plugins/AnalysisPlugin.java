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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.luceneserver.analysis.AnalysisProvider;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;

/**
 * Plugin interface for providing custom analysis implementations. Currently allows for the
 * registration of named {@link Analyzer} providers. These analyzers will be used when the name is
 * provides as the predefined AnalyzerType for queries and field registration.
 */
public interface AnalysisPlugin {

  /**
   * Provides a set of custom {@link Analyzer} to register with the {@link
   * com.yelp.nrtsearch.server.luceneserver.analysis.AnalyzerCreator}. The analyzer name can be used
   * as the predefined AnalyzerType in gRPC requests.
   *
   * @return registration Map for analyzer name to {@link AnalysisProvider}
   */
  default Map<String, AnalysisProvider<? extends Analyzer>> getAnalyzers() {
    return Collections.emptyMap();
  }
}
