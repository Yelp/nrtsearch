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
package com.yelp.nrtsearch.server.doc;

import java.util.Map;

/**
 * Interface for access to a per document map that can be used to pass information between different
 * stages of query processing.
 */
public interface SharedDocContext {

  /**
   * Get the context map for a given lucene document id. This method must at least be thread safe
   * under the condition that calls with the same docId will not be done concurrently. The resulting
   * context map is assumed to not be synchronized.
   *
   * @param docId lucene global document id
   * @return mutable context map
   */
  Map<String, Object> getContext(int docId);
}
