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
package com.yelp.nrtsearch.server.luceneserver.analysis;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;

/**
 * Interface to provide additional nrtsearch related information to components of {@link
 * org.apache.lucene.analysis.custom.CustomAnalyzer}s. The {@link
 * #initializeComponent(LuceneServerConfiguration)} method is called after the custom analyzer is
 * built, but before it is returned to the caller.
 *
 * <p>This is intended mainly to be used by custom implementations registered by plugins (currently
 * only token filters supported). This is needed because the custom analyzer building does not give
 * us direct control over instance creation of its subcomponents.
 */
public interface AnalysisComponent {

  /**
   * Method invoked after the {@link org.apache.lucene.analysis.custom.CustomAnalyzer} is built, but
   * before it is returned to the caller. Provides nrtsearch related context information.
   *
   * @param configuration nrtsearch config accessor
   */
  void initializeComponent(LuceneServerConfiguration configuration);
}
