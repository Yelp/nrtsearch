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
package com.yelp.nrtsearch.server.luceneserver.doc;

import com.yelp.nrtsearch.server.luceneserver.IndexState;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Index level class for providing access to doc values data. Provides a means to get a {@link
 * SegmentDocLookup} bound to single lucene segment.
 */
public class DocLookup {
  private final IndexState indexState;

  public DocLookup(IndexState indexState) {
    this.indexState = indexState;
  }

  /**
   * Get the doc value lookup accessor bound to the given lucene segment.
   *
   * @param context lucene segment context
   * @return lookup accessor for given segment context
   */
  public SegmentDocLookup getSegmentLookup(LeafReaderContext context) {
    return new SegmentDocLookup(indexState, context);
  }

  /**
   * Get the state information associated with this index.
   *
   * @return index state
   */
  public IndexState getIndexState() {
    return indexState;
  }
}
