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
package com.yelp.nrtsearch.server.luceneserver.warming;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Warmer {
  private static final Logger logger = LoggerFactory.getLogger(Warmer.class);

  private final RemoteBackend remoteBackend;
  private final String service;
  private final List<SearchRequest> warmingRequests;
  private final ReservoirSampler reservoirSampler;
  private final String index;
  private final int maxWarmingQueries;

  public Warmer(RemoteBackend remoteBackend, String service, String index, int maxWarmingQueries) {
    this.remoteBackend = remoteBackend;
    this.service = service;
    this.index = index;
    this.warmingRequests = Collections.synchronizedList(new ArrayList<>(maxWarmingQueries));
    this.reservoirSampler = new ReservoirSampler(maxWarmingQueries);
    this.maxWarmingQueries = maxWarmingQueries;
  }

  public int getNumWarmingRequests() {
    return warmingRequests.size();
  }

  public void addSearchRequest(SearchRequest searchRequest) {
    ReservoirSampler.SampleResult sampleResult = reservoirSampler.sample();
    if (sampleResult.isSample()) {
      int replace = sampleResult.getReplace();
      if (warmingRequests.size() < maxWarmingQueries) {
        warmingRequests.add(searchRequest);
      } else {
        warmingRequests.set(replace, searchRequest);
      }
    }
  }

  public synchronized void backupWarmingQueriesToS3(String service) throws IOException {
    if (Strings.isNullOrEmpty(service)) {
      service = this.service;
    }

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    int count = 0;
    try (Writer writer =
        new OutputStreamWriter(byteArrayOutputStream, StateUtils.getValidatingUTF8Encoder())) {
      for (SearchRequest searchRequest : warmingRequests) {
        writer.write(JsonFormat.printer().omittingInsignificantWhitespace().print(searchRequest));
        writer.write("\n");
        count++;
      }
    }

    byte[] warmingQueriesBytes = byteArrayOutputStream.toByteArray();
    remoteBackend.uploadWarmingQueries(service, index, warmingQueriesBytes);
    logger.info(
        "Backed up {} warming queries for index: {} to service: {}, size: {}",
        count,
        index,
        service,
        warmingQueriesBytes.length);
  }

  public void warmFromS3(IndexState indexState, int parallelism)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    SearchHandler searchHandler = new SearchHandler(indexState.getSearchThreadPoolExecutor(), true);
    warmFromS3(indexState, parallelism, searchHandler);
  }

  @VisibleForTesting
  void warmFromS3(IndexState indexState, int parallelism, SearchHandler searchHandler)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    if (!remoteBackend.exists(service, index, IndexResourceType.WARMING_QUERIES)) {
      logger.info("No warming queries found in S3 for service: {} and index: {}", service, index);
      return;
    }
    ThreadPoolExecutor threadPoolExecutor = null;
    if (parallelism > 1) {
      int numThreads = parallelism - 1;
      threadPoolExecutor =
          new ThreadPoolExecutor(
              numThreads,
              numThreads,
              0,
              TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              new NamedThreadFactory("warming-"),
              new ThreadPoolExecutor.CallerRunsPolicy());
    }
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                remoteBackend.downloadWarmingQueries(service, index),
                StateUtils.getValidatingUTF8Decoder()))) {
      String line;
      int count = 0;
      while ((line = reader.readLine()) != null) {
        processLine(indexState, searchHandler, threadPoolExecutor, line);
        count++;
      }
      logger.info("Warmed index: {} with {} warming queries", index, count);
    } finally {
      if (threadPoolExecutor != null) {
        threadPoolExecutor.shutdown();
        threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  private void processLine(
      IndexState indexState,
      SearchHandler searchHandler,
      ThreadPoolExecutor threadPoolExecutor,
      String line)
      throws InvalidProtocolBufferException, SearchHandler.SearchHandlerException {
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    JsonFormat.parser().merge(line, builder);
    SearchRequest searchRequest = builder.build();
    if (threadPoolExecutor == null) {
      searchHandler.handle(indexState, searchRequest);
    } else {
      threadPoolExecutor.submit(() -> searchHandler.handle(indexState, searchRequest));
    }
  }
}
