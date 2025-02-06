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
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
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
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.search.Query;
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
  private final int maxWarmingLuceneQueryOnlyCount;

  public Warmer(Archiver archiver, String service, String index, int maxWarmingQueries) {
    this(archiver, service, index, maxWarmingQueries, 0);
  }

  public Warmer(
      Archiver archiver,
      String service,
      String index,
      int maxWarmingQueries,
      int maxWarmingLuceneQueryOnlyCount) {
    this.archiver = archiver;
    this.service = service;
    this.index = index;
    this.resource = index + WARMING_QUERIES_RESOURCE;
    this.warmingRequests = Collections.synchronizedList(new ArrayList<>(maxWarmingQueries));
    this.reservoirSampler = new ReservoirSampler(maxWarmingQueries);
    this.maxWarmingQueries = maxWarmingQueries;
    this.maxWarmingLuceneQueryOnlyCount = maxWarmingLuceneQueryOnlyCount;
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
      throws IOException, InterruptedException {
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
    long startMS = System.currentTimeMillis();
    try (BufferedReader reader =
        Files.newBufferedReader(warmingRequestsDir.resolve(WARMING_QUERIES_FILE))) {
      String line;
      int count = 0;
      while ((line = reader.readLine()) != null) {
        count++;
        processLine(
            indexState,
            searchHandler,
            threadPoolExecutor,
            line,
            count < maxWarmingLuceneQueryOnlyCount); // warm up the query cache first
      }
      logger.info(
          "Warmed index: {} with {} warming queries in {}ms",
          index,
          count,
          System.currentTimeMillis() - startMS);
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

  private SearcherTaxonomyManager.SearcherAndTaxonomy getSearcherAndTaxonomy(
      SearchRequest searchRequest, IndexState indexState) throws IOException, InterruptedException {
    return SearchHandler.getSearcherAndTaxonomy(
        searchRequest,
        indexState,
        indexState.getShard(0),
        SearchResponse.Diagnostics.newBuilder(),
        indexState.getSearchThreadPoolExecutor());
  }

  private void processLine(
      IndexState indexState,
      SearchHandler searchHandler,
      ThreadPoolExecutor threadPoolExecutor,
      String line,
      boolean luceneQueryOnly)
      throws InvalidProtocolBufferException {
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    JsonFormat.parser().merge(line, builder);
    SearchRequest searchRequest = builder.build();
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;

    try {
      if (luceneQueryOnly) {
        Query luceneQuery =
            QueryNodeMapper.getInstance().getQuery(searchRequest.getQuery(), indexState);
        try {
          s = getSearcherAndTaxonomy(searchRequest, indexState);
          if (threadPoolExecutor == null) {
            s.searcher.search(luceneQuery, 0);
          } else {
            SearcherTaxonomyManager.SearcherAndTaxonomy finalS = s;
            threadPoolExecutor.submit(() -> finalS.searcher.search(luceneQuery, 0));
          }
        } finally {
          if (s != null) {
            try {
              indexState.getShard(0).release(s);
            } catch (IOException e) {
              logger.error("failed closing the search in warmer.", e);
              throw e;
            }
          }
        }
      } else if (threadPoolExecutor == null) {
        searchHandler.handle(indexState, searchRequest);
      } else {
        threadPoolExecutor.submit(() -> searchHandler.handle(indexState, searchRequest));
      }
    } catch (IOException | InterruptedException | SearchHandler.SearchHandlerException e) {
      logger.warn("failed a warming querying");
    }
  }
}
