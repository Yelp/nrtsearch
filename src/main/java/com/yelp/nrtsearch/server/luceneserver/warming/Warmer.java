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
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  public static final String WARMING_QUERIES_RESOURCE = "_warming_queries";
  public static final String WARMING_QUERIES_DIR = "warming_queries";
  private static final String WARMING_QUERIES_FILE = "warming_queries.txt";

  private final Archiver archiver;
  private final String service;
  private final String resource;
  private final List<SearchRequest> warmingRequests;
  private final ReservoirSampler reservoirSampler;
  private final String index;
  private final int maxWarmingQueries;

  public Warmer(Archiver archiver, String service, String index, int maxWarmingQueries) {
    this.archiver = archiver;
    this.service = service;
    this.index = index;
    this.resource = index + WARMING_QUERIES_RESOURCE;
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
    // TODO: tmpDirectory used for simplicity but we might want to provide directory via config
    Path tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"));
    Path warmingQueriesDir = null;
    Path warmingQueriesFile = null;
    BufferedWriter writer = null;
    try {
      // Creating a directory since the Archiver requires a directory
      warmingQueriesDir = Files.createDirectory(tmpDirectory.resolve(WARMING_QUERIES_DIR));
      warmingQueriesFile = warmingQueriesDir.resolve(WARMING_QUERIES_FILE);
      writer = Files.newBufferedWriter(warmingQueriesFile);
      int count = 0;
      for (SearchRequest searchRequest : warmingRequests) {
        writer.write(JsonFormat.printer().omittingInsignificantWhitespace().print(searchRequest));
        writer.newLine();
        count++;
      }
      writer.close();
      writer = null;
      String versionHash =
          archiver.upload(service, resource, warmingQueriesDir, List.of(), List.of(), true);
      archiver.blessVersion(service, resource, versionHash);
      logger.info(
          "Backed up {} warming queries for index: {} to service: {}, resource: {}",
          count,
          index,
          service,
          resource);
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (warmingQueriesFile != null && Files.exists(warmingQueriesFile)) {
        Files.delete(warmingQueriesFile);
      }
      if (warmingQueriesDir != null && Files.exists(warmingQueriesDir)) {
        Files.delete(warmingQueriesDir);
      }
    }
  }

  public void warmFromS3(IndexState indexState, int parallelism)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    SearchHandler searchHandler = new SearchHandler(indexState.getSearchThreadPoolExecutor(), true);
    warmFromS3(indexState, parallelism, searchHandler);
  }

  @VisibleForTesting
  void warmFromS3(IndexState indexState, int parallelism, SearchHandler searchHandler)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    if (archiver.getVersionedResource(service, resource).isEmpty()) {
      logger.info(
          "No warming queries found in S3 for service: {} and resource: {}", service, resource);
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
    Path downloadDir = archiver.download(service, resource);
    Path warmingRequestsDir = downloadDir.resolve(WARMING_QUERIES_DIR);
    try (BufferedReader reader =
        Files.newBufferedReader(warmingRequestsDir.resolve(WARMING_QUERIES_FILE))) {
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
      // Leave warming files on disk, for use if the index is restarted.
      // If we find these files end up being too large, we could consider adding
      // some kind of local resource cache purging to the Archiver interface.
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
