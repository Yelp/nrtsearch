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
package com.yelp.nrtsearch.server.luceneserver.index;

import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Similarity wrapper for use at indexing time, which finds the proper per field similarity. This
 * similarity is set in the {@link org.apache.lucene.index.IndexWriterConfig} when an index is
 * started. The {@link IndexStateManager} provides access to the fields in the latest {@link
 * com.yelp.nrtsearch.server.luceneserver.IndexState}.
 */
public class IndexSimilarity extends PerFieldSimilarityWrapper {
  private static final Similarity DEFAULT_SIMILARITY = new BM25Similarity();
  private final IndexStateManager stateManager;

  public IndexSimilarity(IndexStateManager stateManager) {
    this.stateManager = stateManager;
  }

  @Override
  public Similarity get(String name) {
    return getFromState(name, stateManager.getCurrent());
  }

  /**
   * Get the similarity implementation for a given field name from the given index state. If the
   * field is not found, defaults to {@link BM25Similarity}.
   *
   * @param name field name
   * @param indexState index state
   * @return similarity implementation
   */
  public static Similarity getFromState(String name, IndexState indexState) {
    try {
      FieldDef fd = indexState.getField(name);
      if (fd instanceof IndexableFieldDef) {
        return ((IndexableFieldDef) fd).getSimilarity();
      }
    } catch (IllegalArgumentException ignored) {
      // ReplicaNode tries to do a Term query for a field called 'marker'
      // in finishNRTCopy. Since the field is not in the index, we want
      // to ignore the exception.
    }
    return DEFAULT_SIMILARITY;
  }
}
