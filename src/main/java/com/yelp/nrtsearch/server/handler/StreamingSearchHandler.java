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
package com.yelp.nrtsearch.server.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.StreamingSearchResponse;
import com.yelp.nrtsearch.server.grpc.StreamingSearchResponseHeader;
import com.yelp.nrtsearch.server.grpc.StreamingSearchResponseHits;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.search.SearchContext;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.ProtoMessagePrinter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for streaming search requests. Returns search results as a stream where: - First message
 * contains all response metadata (diagnostics, facets, total hits, etc.) but no hits - Subsequent
 * messages contain batches of hits
 *
 * <p>The batch size can be controlled via SearchRequest.streamingHitBatchSize (default: 50).
 */
public class StreamingSearchHandler extends Handler<SearchRequest, StreamingSearchResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StreamingSearchHandler.class);
  private static final int DEFAULT_BATCH_SIZE = 50;

  private final SearchHandler searchHandler;

  public StreamingSearchHandler(GlobalState globalState) {
    super(globalState);
    this.searchHandler = new SearchHandler(globalState);
  }

  @Override
  public void handle(
      SearchRequest searchRequest, StreamObserver<StreamingSearchResponse> responseObserver) {
    SearchHandler.SearchExecutionResult execResult = null;

    try {
      IndexState indexState = getGlobalState().getIndex(searchRequest.getIndexName());
      setResponseCompression(searchRequest.getResponseCompression(), responseObserver);

      // Execute search to get doc IDs, collector results, metadata
      execResult = searchHandler.executeSearchPhase(indexState, searchRequest);

      // Send header immediately with metadata
      StreamingSearchResponseHeader header = buildResponseHeader(execResult);
      StreamingSearchResponse headerMessage =
          StreamingSearchResponse.newBuilder().setHeader(header).build();
      responseObserver.onNext(headerMessage);

      streamHitsProgressively(execResult, searchRequest, responseObserver);

      // Add searchRequest to warmer if needed
      try {
        if (indexState.getWarmer() != null) {
          indexState.getWarmer().addSearchRequest(searchRequest);
        }
      } catch (Exception e) {
        logger.error("Unable to add warming query", e);
      }

      responseObserver.onCompleted();

    } catch (Exception e) {
      String requestStr;
      try {
        requestStr = ProtoMessagePrinter.omittingInsignificantWhitespace().print(searchRequest);
      } catch (InvalidProtocolBufferException ignored) {
        requestStr = searchRequest.toString();
      }
      logger.warn("Error handling streaming search request: {}", requestStr, e);

      if (e instanceof StatusRuntimeException) {
        responseObserver.onError(e);
      } else {
        responseObserver.onError(
            Status.INTERNAL
                .withDescription(
                    String.format(
                        "Error while trying to execute streaming search for index %s. check logs for full searchRequest.",
                        searchRequest.getIndexName()))
                .augmentDescription(e.getMessage())
                .asRuntimeException());
      }
    } finally {
      // Release searcher reference
      if (execResult != null) {
        try {
          execResult.shardState().release(execResult.searcherAndTaxonomy());
        } catch (IOException e) {
          logger.warn("Failed to release searcher reference", e);
        }
      }
    }
  }

  /**
   * Build the response header from the search execution result. The header contains search metadata
   * but no hits - those will be streamed separately.
   */
  private StreamingSearchResponseHeader buildResponseHeader(
      SearchHandler.SearchExecutionResult execResult) {
    StreamingSearchResponseHeader.Builder headerBuilder =
        StreamingSearchResponseHeader.newBuilder();

    SearchContext searchContext = execResult.searchContext();

    headerBuilder.setDiagnostics(execResult.diagnostics());
    headerBuilder.setHitTimeout(searchContext.getCollector().hadTimeout());
    headerBuilder.setTotalHits(execResult.totalHits());

    // TODO: searchState doesn't include the
    SearchResponse.SearchState.Builder searchState = SearchResponse.SearchState.newBuilder();
    searchState.setTimestamp(searchContext.getTimestampSec());
    searchState.setSearcherVersion(
        ((DirectoryReader) execResult.searcherAndTaxonomy().searcher().getIndexReader())
            .getVersion());
    headerBuilder.setSearchState(searchState);

    headerBuilder.addAllFacetResult(searchContext.getResponseBuilder().getFacetResultList());

    if (execResult.profileResult() != null) {
      headerBuilder.setProfileResult(execResult.profileResult());
    }

    headerBuilder.putAllCollectorResults(
        searchContext.getResponseBuilder().getCollectorResultsMap());
    headerBuilder.setTerminatedEarly(searchContext.getCollector().terminatedEarly());

    return headerBuilder.build();
  }

  /**
   * Stream hits progressively as fields are fetched. This method fetches fields in batches and
   * streams each batch as soon as it's ready, reducing latency and memory usage.
   *
   * @param execResult the search execution result containing doc IDs
   * @param searchRequest the original search request
   * @param responseObserver the observer to send batched hits to
   */
  private void streamHitsProgressively(
      SearchHandler.SearchExecutionResult execResult,
      SearchRequest searchRequest,
      StreamObserver<StreamingSearchResponse> responseObserver)
      throws Exception {

    SearchContext searchContext = execResult.searchContext();
    TopDocs topDocs = execResult.topDocs();

    TopDocs hits =
        SearchHandler.getHitsFromOffset(
            topDocs,
            searchContext.getStartHit(),
            Math.max(
                searchContext.getTopHits(),
                searchContext.getHitsToLog() + searchContext.getStartHit()));

    if (hits.scoreDocs.length == 0) {
      // No hits to stream
      return;
    }

    // Determine batch size
    int batchSize = searchRequest.getStreamingHitBatchSize();
    if (batchSize <= 0) {
      batchSize = DEFAULT_BATCH_SIZE;
    }

    // Process and stream hits in batches
    for (int startIdx = 0; startIdx < hits.scoreDocs.length; startIdx += batchSize) {
      int endIdx = Math.min(startIdx + batchSize, hits.scoreDocs.length);

      List<SearchResponse.Hit.Builder> hitBuilders = new ArrayList<>(endIdx - startIdx);
      for (int i = startIdx; i < endIdx; i++) {
        ScoreDoc scoreDoc = hits.scoreDocs[i];
        SearchResponse.Hit.Builder hitBuilder = SearchResponse.Hit.newBuilder();
        hitBuilder.setLuceneDocId(scoreDoc.doc);
        searchContext.getCollector().fillHitRanking(hitBuilder, scoreDoc);
        hitBuilders.add(hitBuilder);
      }

      fetchFieldsForBatch(searchContext, hitBuilders);

      List<SearchResponse.Hit> batchHits = new ArrayList<>(hitBuilders.size());
      for (SearchResponse.Hit.Builder hitBuilder : hitBuilders) {
        batchHits.add(hitBuilder.build());
      }
      sendHitBatch(batchHits, responseObserver);
    }
  }

  /**
   * Fetch fields for a batch of hits. Uses the same logic as SearchHandler.fetchFields() but
   * operates on a subset of hits.
   */
  private void fetchFieldsForBatch(
      SearchContext searchContext, List<SearchResponse.Hit.Builder> hitBuilders) throws Exception {
    if (hitBuilders.isEmpty()) {
      return;
    }

    // Sort hits by lucene doc id for efficient field fetching
    hitBuilders.sort(Comparator.comparing(SearchResponse.Hit.Builder::getLuceneDocId));

    SearchHandler.FillDocsTask fillDocsTask =
        new SearchHandler.FillDocsTask(searchContext, hitBuilders, searchContext.getQuery());
    fillDocsTask.run();
  }

  /** Send a batch of hits to the client. */
  private void sendHitBatch(
      List<SearchResponse.Hit> hits, StreamObserver<StreamingSearchResponse> responseObserver) {
    StreamingSearchResponseHits hitsMessage =
        StreamingSearchResponseHits.newBuilder().addAllHits(hits).build();
    StreamingSearchResponse response =
        StreamingSearchResponse.newBuilder().setHits(hitsMessage).build();
    responseObserver.onNext(response);
  }
}
