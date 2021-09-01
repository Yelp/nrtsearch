/*
 * Copyright 2021 Yelp Inc.
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

import java.io.IOException;
import org.apache.lucene.search.TopDocs;

/**
 * Interface for implementation of a rescore operation that produces {@link TopDocs} given the
 * {@link TopDocs} from a previous search/rescore operation.
 */
public interface RescoreOperation {

  /**
   * Rescore the given hits to produce a new set of {@link TopDocs}.
   *
   * @param hits {@link TopDocs} from a previous search/rescore operation
   * @param context additional context for rescoring
   * @return new hits ranking
   * @throws IOException on error loading index data
   */
  TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException;
}
