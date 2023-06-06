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
import com.yelp.nrtsearch.server.luceneserver.highlights.HighlightFetchTask;
import com.yelp.nrtsearch.server.luceneserver.innerhit.InnerHitFetchTask;
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
   * <p>c) The {@link HighlightFetchTask#processHit(SearchContext, LeafReaderContext,
   * SearchResponse.Hit.Builder)} and {@link InnerHitFetchTask#processHit(SearchContext,
   * LeafReaderContext, SearchResponse.Hit.Builder)} method is called for each {@link FetchTask} in
   * order
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
     * populated, as well as all requested query fields. The method will be called in order of
     * lucene doc ID of the hits.
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

  // TopHitsCollector supports highlightFetchTask only for now. Use this to retrieve
  // highlightFetchTasks only.
  private HighlightFetchTask highlightFetchTask;
  private List<InnerHitFetchTask> innerHitFetchTaskList;

  public HighlightFetchTask getHighlightFetchTask() {
    return highlightFetchTask;
  }

  public List<InnerHitFetchTask> getInnerHitFetchTaskList() {
    return innerHitFetchTaskList;
  }

  public void setHighlightFetchTask(HighlightFetchTask highlightFetchTask) {
    this.highlightFetchTask = highlightFetchTask;
  }

  public void setInnerHitFetchTaskList(List<InnerHitFetchTask> innerHitFetchTaskList) {
    this.innerHitFetchTaskList = innerHitFetchTaskList;
  }

  /**
   * Constructor.
   *
   * @param grpcTaskList fetch task definitions from search request
   */
  public FetchTasks(List<com.yelp.nrtsearch.server.grpc.FetchTask> grpcTaskList) {
    this(grpcTaskList, null, null);
  }

  /**
   * Constructor.
   *
   * @param grpcTaskList fetch task definitions from search request
   * @param highlightFetchTask highlight fetch task
   * @param innerHitFetchTaskList innerHit fetch tasks
   */
  public FetchTasks(
      List<com.yelp.nrtsearch.server.grpc.FetchTask> grpcTaskList,
      HighlightFetchTask highlightFetchTask,
      List<InnerHitFetchTask> innerHitFetchTaskList) {
    taskList =
        grpcTaskList.stream()
            .map(
                t ->
                    com.yelp.nrtsearch.server.luceneserver.search.FetchTaskCreator.getInstance()
                        .createFetchTask(t))
            .collect(Collectors.toList());
    this.highlightFetchTask = highlightFetchTask;
    this.innerHitFetchTaskList = innerHitFetchTaskList;
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
    // highlight and innerHit doesn't support processAllHits now
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
    if (highlightFetchTask != null) {
      highlightFetchTask.processHit(searchContext, segment, hit);
    }
    if (innerHitFetchTaskList != null) {
      for (InnerHitFetchTask innerHitFetchTask : innerHitFetchTaskList) {
        innerHitFetchTask.processHit(searchContext, segment, hit);
      }
    }
  }
}
