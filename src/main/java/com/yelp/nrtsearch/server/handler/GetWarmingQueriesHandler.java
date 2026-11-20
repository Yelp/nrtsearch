/*
 * Copyright 2026 Yelp Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.GetWarmingQueriesRequest;
import com.yelp.nrtsearch.server.grpc.WarmingQueriesChunk;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.warming.Warmer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends this node's warming corpus to a starting peer replica, for the PEER warming strategy. The
 * corpus is the same as the one the backup job uploads to S3: queries sampled from live traffic,
 * padded with downloaded queries when enabled.
 */
public class GetWarmingQueriesHandler
    extends Handler<GetWarmingQueriesRequest, WarmingQueriesChunk> {
  private static final Logger logger = LoggerFactory.getLogger(GetWarmingQueriesHandler.class);
  // well under the 1 GB max message size, small enough to not hold much memory per chunk
  static final int CHUNK_BYTES = 1024 * 1024;

  private final LongSupplier uptimeMsSupplier;

  public GetWarmingQueriesHandler(GlobalState globalState) {
    this(globalState, () -> ManagementFactory.getRuntimeMXBean().getUptime());
  }

  @VisibleForTesting
  GetWarmingQueriesHandler(GlobalState globalState, LongSupplier uptimeMsSupplier) {
    super(globalState);
    this.uptimeMsSupplier = uptimeMsSupplier;
  }

  @Override
  public void handle(
      GetWarmingQueriesRequest request, StreamObserver<WarmingQueriesChunk> responseObserver) {
    String index = request.getIndexName();
    try {
      IndexState indexState = getIndexState(index);
      Warmer warmer = indexState.getWarmer();
      if (warmer == null) {
        throw Status.FAILED_PRECONDITION
            .withDescription("No warmer for index: " + index)
            .asRuntimeException();
      }
      long uptimeMinutes = uptimeMsSupplier.getAsLong() / 1000L / 60L;
      if (request.getMinUptimeMinutes() > 0 && uptimeMinutes < request.getMinUptimeMinutes()) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                String.format(
                    "Uptime is %d minutes, less than threshold %d",
                    uptimeMinutes, request.getMinUptimeMinutes()))
            .asRuntimeException();
      }
      int corpusSize = warmer.getWarmingCorpusSize();
      if (request.getMinQueries() > 0 && corpusSize < request.getMinQueries()) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                String.format(
                    "Has %d warming queries, less than threshold %d",
                    corpusSize, request.getMinQueries()))
            .asRuntimeException();
      }
      int sent =
          warmer.writeWarmingCorpus(
              request.getMaxQueries(),
              request.getMaxBytes(),
              CHUNK_BYTES,
              chunk ->
                  responseObserver.onNext(
                      WarmingQueriesChunk.newBuilder()
                          .setContent(ByteString.copyFrom(chunk))
                          .build()));
      responseObserver.onCompleted();
      logger.info("Sent {} warming queries for index: {} to a peer", sent, index);
    } catch (StatusRuntimeException e) {
      logger.info("Not sending warming queries for index: {}: {}", index, e.getMessage());
      responseObserver.onError(e);
    } catch (Exception e) {
      logger.warn("Error sending warming queries for index: {}", index, e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error sending warming queries for index: " + index)
              .augmentDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    }
  }
}
