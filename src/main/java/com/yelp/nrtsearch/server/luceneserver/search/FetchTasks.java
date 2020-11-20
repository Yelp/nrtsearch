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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.Builder;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;

/** Class that manages the execution of custom {@link FetchTask}s. */
public class FetchTasks {

  /**
   * Interface for a custom task that should be run while fetching field values. There are two
   * separate methods that may be optionally implemented. The order of operations is as follows:
   *
   * <p>1) For each top document:
   *
   * <p>a) The document has it's query specified fields filled
   *
   * <p>b) The {@link FetchTask#processHit(SearchContext, LeafReaderContext,
   * SearchResponse.Hit.Builder)} method is called for each {@link FetchTask} in order
   *
   * <p>2) The {@link FetchTask#processAllHits(SearchContext, List)} method is called for each
   * {@link FetchTask} in order
   */
  public interface FetchTask {
    /**
     * Process the list of all query hits. This is the final fetch operation.
     *
     * @param searchContext search context
     * @param hits query hits
     * @throws IOException on error reading data
     */
    default void processAllHits(SearchContext searchContext, List<SearchResponse.Hit.Builder> hits)
        throws IOException {}

    /**
     * Process each hit individually. Hits will already have lucene doc id and scoring info
     * populated, as well as all requested query fields.
     *
     * @param searchContext search context
     * @param hitLeaf lucene segment for hit
     * @param hit hit builder for query response
     * @throws IOException on error reading data
     */
    default void processHit(
        SearchContext searchContext, LeafReaderContext hitLeaf, SearchResponse.Hit.Builder hit)
        throws IOException {}
  }

  private final List<FetchTask> taskList;

  /**
   * Constructor.
   *
   * @param grpcTaskList fetch task definitions from search request
   */
  public FetchTasks(List<com.yelp.nrtsearch.server.grpc.FetchTask> grpcTaskList) {
    taskList =
        grpcTaskList.stream()
            .map(
                t ->
                    com.yelp.nrtsearch.server.luceneserver.search.FetchTaskCreator.getInstance()
                        .createFetchTask(t))
            .collect(Collectors.toList());
  }

  /**
   * Invoke the {@link FetchTask#processAllHits(SearchContext, List)} method on all query {@link
   * FetchTask}s.
   *
   * @param searchContext search context
   * @param hits list of query hits
   * @throws IOException on error reading data
   */
  public void processAllHits(SearchContext searchContext, List<SearchResponse.Hit.Builder> hits)
      throws IOException {
    for (FetchTask task : taskList) {
      task.processAllHits(searchContext, hits);
    }
  }

  /**
   * Invoke the {@link FetchTask#processHit(SearchContext, LeafReaderContext, Builder)} method for
   * this hit on all query {@link FetchTask}s.
   *
   * @param searchContext search context
   * @param segment lucene segment for this hit
   * @param hit hit builder for query response
   * @throws IOException on error reading data
   */
  public void processHit(
      SearchContext searchContext, LeafReaderContext segment, SearchResponse.Hit.Builder hit)
      throws IOException {
    for (FetchTask task : taskList) {
      task.processHit(searchContext, segment, hit);
    }
  }
}
