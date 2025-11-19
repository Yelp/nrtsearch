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
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.ProtoMessagePrinter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
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
    try {
      // Get the complete search response using the existing SearchHandler
      SearchResponse searchResponse = searchHandler.getSearchResponse(searchRequest);

      // Set response compression if requested
      setResponseCompression(searchRequest.getResponseCompression(), responseObserver);

      // Send the header first (all metadata, no hits)
      StreamingSearchResponseHeader header = buildResponseHeader(searchResponse);
      StreamingSearchResponse headerMessage =
          StreamingSearchResponse.newBuilder().setHeader(header).build();
      responseObserver.onNext(headerMessage);

      // Stream the hits in batches
      streamHits(searchResponse, searchRequest, responseObserver);

      // Complete the stream
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
    }
  }

  /**
   * Build the response header from the full search response. The header contains everything except
   * the hits.
   */
  private StreamingSearchResponseHeader buildResponseHeader(SearchResponse searchResponse) {
    StreamingSearchResponseHeader.Builder headerBuilder =
        StreamingSearchResponseHeader.newBuilder();

    // Copy all fields from SearchResponse except hits
    headerBuilder.setDiagnostics(searchResponse.getDiagnostics());
    headerBuilder.setHitTimeout(searchResponse.getHitTimeout());
    headerBuilder.setTotalHits(searchResponse.getTotalHits());

    if (searchResponse.hasSearchState()) {
      headerBuilder.setSearchState(searchResponse.getSearchState());
    }

    headerBuilder.addAllFacetResult(searchResponse.getFacetResultList());

    if (searchResponse.hasProfileResult()) {
      headerBuilder.setProfileResult(searchResponse.getProfileResult());
    }

    headerBuilder.putAllCollectorResults(searchResponse.getCollectorResultsMap());
    headerBuilder.setTerminatedEarly(searchResponse.getTerminatedEarly());

    return headerBuilder.build();
  }

  /**
   * Stream hits in batches to the client.
   *
   * @param searchResponse the complete search response containing all hits
   * @param searchRequest the original search request
   * @param responseObserver the observer to send batched hits to
   */
  private void streamHits(
      SearchResponse searchResponse,
      SearchRequest searchRequest,
      StreamObserver<StreamingSearchResponse> responseObserver) {

    List<SearchResponse.Hit> allHits = searchResponse.getHitsList();
    if (allHits.isEmpty()) {
      // No hits to stream
      return;
    }

    // Determine batch size
    int batchSize = searchRequest.getStreamingHitBatchSize();
    if (batchSize <= 0) {
      batchSize = DEFAULT_BATCH_SIZE;
    }

    // Stream hits in batches
    List<SearchResponse.Hit> batch = new ArrayList<>(batchSize);
    for (SearchResponse.Hit hit : allHits) {
      batch.add(hit);

      if (batch.size() >= batchSize) {
        sendHitBatch(batch, responseObserver);
        batch.clear();
      }
    }

    // Send any remaining hits
    if (!batch.isEmpty()) {
      sendHitBatch(batch, responseObserver);
    }
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
