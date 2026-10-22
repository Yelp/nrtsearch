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
package com.yelp.nrtsearch.server.warming;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.grpc.DeadlineUtils;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.handler.SearchHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.monitoring.BootstrapMetrics;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType;
import com.yelp.nrtsearch.server.state.StateUtils;
import com.yelp.nrtsearch.server.warming.WarmingQueryFilter.Reason;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.prometheus.metrics.core.datapoints.Timer;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Warmer {
  private static final Logger logger = LoggerFactory.getLogger(Warmer.class);
  // Check the per-query timeout after this many documents are collected, so a single large
  // segment cannot hold a replayed query past its timeout
  static final int REPLAY_TIMEOUT_CHECK_EVERY = 1000;
  // Log only the first few replay failures, a broken corpus could otherwise produce thousands
  private static final int MAX_LOGGED_REPLAY_FAILURES = 10;

  private final RemoteBackend remoteBackend;
  private final String service;
  private final List<SearchRequest> warmingRequests;
  private volatile List<SearchRequest> downloadedWarmingRequests = Collections.emptyList();
  private final ReservoirSampler reservoirSampler;
  private final String index;
  private final int maxWarmingQueries;
  private final int warmBasicQueryOnlyPerc;
  private final boolean padWithDownloadedRequests;
  private final WarmingQueryFilter queryFilter;
  private final int queryTimeoutMs;
  protected final ThreadLocal<Random> randomThreadLocal;

  public Warmer(RemoteBackend remoteBackend, String service, String index, int maxWarmingQueries) {
    this(remoteBackend, service, index, maxWarmingQueries, 0);
  }

  public Warmer(
      RemoteBackend remoteBackend,
      String service,
      String index,
      int maxWarmingQueries,
      int warmBasicQueryOnlyPerc) {
    this(remoteBackend, service, index, maxWarmingQueries, warmBasicQueryOnlyPerc, false);
  }

  public Warmer(
      RemoteBackend remoteBackend,
      String service,
      String index,
      int maxWarmingQueries,
      int warmBasicQueryOnlyPerc,
      boolean padWithDownloadedRequests) {
    this(
        remoteBackend,
        service,
        index,
        maxWarmingQueries,
        warmBasicQueryOnlyPerc,
        padWithDownloadedRequests,
        WarmingQueryFilter.acceptAll(index),
        0);
  }

  /**
   * @param remoteBackend backend to store warming queries
   * @param service service name
   * @param index index name
   * @param maxWarmingQueries maximum queries to store for warming
   * @param warmBasicQueryOnlyPerc percentage of warming queries that should be basic queries
   * @param padWithDownloadedRequests pad backup with downloaded queries when fewer were observed
   * @param queryFilter filter for queries that should not be part of the warming corpus
   * @param queryTimeoutMs timeout for each replayed warming query, 0 to disable
   */
  public Warmer(
      RemoteBackend remoteBackend,
      String service,
      String index,
      int maxWarmingQueries,
      int warmBasicQueryOnlyPerc,
      boolean padWithDownloadedRequests,
      WarmingQueryFilter queryFilter,
      int queryTimeoutMs) {
    this.remoteBackend = remoteBackend;
    this.service = service;
    this.index = index;
    this.warmingRequests = Collections.synchronizedList(new ArrayList<>(maxWarmingQueries));
    this.reservoirSampler = new ReservoirSampler(maxWarmingQueries);
    this.maxWarmingQueries = maxWarmingQueries;
    this.warmBasicQueryOnlyPerc = warmBasicQueryOnlyPerc;
    this.padWithDownloadedRequests = padWithDownloadedRequests;
    this.queryFilter = queryFilter;
    this.queryTimeoutMs = queryTimeoutMs;
    this.randomThreadLocal = ThreadLocal.withInitial(Random::new);
  }

  public int getNumWarmingRequests() {
    return warmingRequests.size();
  }

  /**
   * Add a query observed in live traffic to the warming corpus, if it is chosen by reservoir
   * sampling and accepted by the query filter. The filter only runs for sampled queries, keeping it
   * off the hot path once the reservoir is full. Sampling stays uniform over accepted queries,
   * since the filter decision does not depend on the sampler.
   *
   * @param searchRequest query to add
   * @param latencyMs time taken to execute the query
   * @param docLookup lookup for the index fields, used by the query filter, may be null
   */
  public void maybeAddSearchRequest(
      SearchRequest searchRequest, double latencyMs, DocLookup docLookup) {
    ReservoirSampler.SampleResult sampleResult = reservoirSampler.sample();
    if (sampleResult.isSample()
        && queryFilter.checkCollected(searchRequest, latencyMs, docLookup) == null) {
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
    List<SearchRequest> observedRequests;
    synchronized (warmingRequests) {
      observedRequests = new ArrayList<>(warmingRequests);
    }
    // Observed queries were filtered when collected, and downloaded queries when replayed
    List<SearchRequest> warmingRequestsToBackup;
    if (padWithDownloadedRequests) {
      List<SearchRequest> downloaded = this.downloadedWarmingRequests;
      int deficit = maxWarmingQueries - observedRequests.size();
      if (deficit > 0 && !downloaded.isEmpty()) {
        int fromIndex = Math.max(0, downloaded.size() - deficit);
        List<SearchRequest> padding = downloaded.subList(fromIndex, downloaded.size());
        warmingRequestsToBackup = new ArrayList<>(padding.size() + observedRequests.size());
        warmingRequestsToBackup.addAll(padding);
        warmingRequestsToBackup.addAll(observedRequests);
      } else {
        warmingRequestsToBackup = observedRequests;
      }
    } else {
      warmingRequestsToBackup = observedRequests;
    }
    try (Writer writer =
        new OutputStreamWriter(byteArrayOutputStream, StateUtils.getValidatingUTF8Encoder())) {
      for (SearchRequest searchRequest : warmingRequestsToBackup) {
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
    warmFromS3(indexState, parallelism, 0);
  }

  /**
   * Replay the warming queries stored in S3.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param maxDurationMs stop starting new queries after this long, 0 for no limit. The remaining
   *     stored queries are still read, so they can pad the next backup.
   */
  public void warmFromS3(IndexState indexState, int parallelism, long maxDurationMs)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    try (Timer _timer =
        BootstrapMetrics.warmingQueryTimer.labelValues(service, index).startTimer()) {
      SearchHandler searchHandler =
          new SearchHandler(indexState.getGlobalState(), indexState.getSearchExecutor(), true);
      warmFromS3(indexState, parallelism, searchHandler, maxDurationMs);
    }
  }

  @VisibleForTesting
  void warmFromS3(IndexState indexState, int parallelism, SearchHandler searchHandler)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    warmFromS3(indexState, parallelism, searchHandler, 0);
  }

  @VisibleForTesting
  void warmFromS3(
      IndexState indexState, int parallelism, SearchHandler searchHandler, long maxDurationMs)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    if (!remoteBackend.exists(service, index, IndexResourceType.WARMING_QUERIES)) {
      logger.info("No warming queries found in S3 for service: {} and index: {}", service, index);
      return;
    }
    ThreadPoolExecutor threadPoolExecutor = null;
    ScheduledExecutorService deadlineScheduler = null;
    long startMS = System.currentTimeMillis();
    long startNs = System.nanoTime();
    // the request deadline only has an effect when deadline cancellation is enabled
    if (queryTimeoutMs > 0 && DeadlineUtils.getCancellationEnabled()) {
      deadlineScheduler =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("warming-deadline-%d")
                  .setDaemon(true)
                  .build());
    }
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
    List<SearchRequest> downloadedQueries = padWithDownloadedRequests ? new ArrayList<>() : null;
    ReplayStats replayStats = new ReplayStats();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                remoteBackend.downloadWarmingQueries(service, index),
                StateUtils.getValidatingUTF8Decoder()))) {
      String line;
      int count = 0, basicCount = 0;
      int skipped = 0;
      boolean reachedMaxDuration = false;
      while ((line = reader.readLine()) != null) {
        if (!reachedMaxDuration
            && maxDurationMs > 0
            && System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(maxDurationMs)) {
          reachedMaxDuration = true;
        }
        if (reachedMaxDuration && downloadedQueries == null) {
          break;
        }
        boolean isStripped = randomThreadLocal.get().nextInt(100) < warmBasicQueryOnlyPerc;
        int position = downloadedQueries == null ? count : downloadedQueries.size();
        SearchRequest originalRequest =
            processLine(
                indexState,
                searchHandler,
                threadPoolExecutor,
                deadlineScheduler,
                line,
                isStripped,
                position,
                !reachedMaxDuration,
                replayStats);
        if (originalRequest != null && downloadedQueries != null) {
          downloadedQueries.add(originalRequest);
        }
        if (reachedMaxDuration) {
          skipped++;
          continue;
        }
        count++;
        if (isStripped) {
          basicCount++;
        }
      }
      // wait for in flight queries, so the replay results are known before they are used
      shutdownAndWait(threadPoolExecutor);
      if (reachedMaxDuration) {
        logger.warn(
            "Warming index: {} reached max warming duration of {} ms, skipped replaying {}"
                + " remaining queries",
            index,
            maxDurationMs,
            skipped);
      }
      logger.info(
          "Warmed index: {} with {} full and {} basic warming queries in {} seconds, timed out: {},"
              + " failed: {}, filtered: {}, basic query skipped as match all: {}.",
          index,
          count - basicCount,
          basicCount,
          (System.currentTimeMillis() - startMS) / 1000.0,
          replayStats.timedOut.get(),
          replayStats.failed.get(),
          replayStats.filtered.get(),
          replayStats.basicMatchAll.get());
      if (downloadedQueries != null) {
        // queries that timed out or failed during replay are not used to pad the next backup,
        // filtered and unparsable queries were never added
        List<SearchRequest> replayable = new ArrayList<>(downloadedQueries.size());
        for (int i = 0; i < downloadedQueries.size(); ++i) {
          if (!replayStats.rejectedPositions.contains(i)) {
            replayable.add(downloadedQueries.get(i));
          }
        }
        this.downloadedWarmingRequests = Collections.unmodifiableList(replayable);
      }
    } finally {
      shutdownAndWait(threadPoolExecutor);
      if (deadlineScheduler != null) {
        deadlineScheduler.shutdownNow();
      }
    }
  }

  private static void shutdownAndWait(ThreadPoolExecutor threadPoolExecutor)
      throws InterruptedException {
    if (threadPoolExecutor != null) {
      threadPoolExecutor.shutdown();
      threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /** Outcome of replayed queries, updated concurrently by warming threads. */
  private static class ReplayStats {
    final AtomicInteger timedOut = new AtomicInteger();
    final AtomicInteger failed = new AtomicInteger();
    final AtomicInteger filtered = new AtomicInteger();
    final AtomicInteger basicMatchAll = new AtomicInteger();
    // position in the downloaded queries of queries that timed out or failed
    final Set<Integer> rejectedPositions = ConcurrentHashMap.newKeySet();
  }

  /**
   * Parse and filter a stored query, and replay it if requested.
   *
   * @return the stored query, or null if it could not be parsed or was rejected by the filter
   */
  private SearchRequest processLine(
      IndexState indexState,
      SearchHandler searchHandler,
      ThreadPoolExecutor threadPoolExecutor,
      ScheduledExecutorService deadlineScheduler,
      String line,
      boolean warmBasicQuery,
      int position,
      boolean shouldReplay,
      ReplayStats replayStats) {
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    try {
      // the corpus may have been written by a newer version with fields this server doesn't know
      JsonFormat.parser().ignoringUnknownFields().merge(line, builder);
    } catch (InvalidProtocolBufferException e) {
      int failed = replayStats.failed.incrementAndGet();
      if (failed <= MAX_LOGGED_REPLAY_FAILURES) {
        logger.warn("Unable to parse warming query for index: {}", index, e);
      }
      WarmingMetrics.warmingQueryCount
          .labelValues(index, warmBasicQuery ? "basic" : "full", "error")
          .inc();
      queryFilter.recordReplayRejection(Reason.ERROR);
      return null;
    }
    SearchRequest originalRequest = builder.build();
    DocLookup docLookup = indexState.docLookup;
    // The stored corpus may predate the filter, so check it before replay. A query that fails is
    // also not used to pad the next backup.
    if (queryFilter.checkReplay(originalRequest, docLookup) != null) {
      replayStats.filtered.incrementAndGet();
      return null;
    }
    if (!shouldReplay) {
      return originalRequest;
    }
    if (warmBasicQuery) {
      WarmingUtils.simplifySearchRequestForWarming(builder);
      // Simplifying can turn a query into a match all scan, such as a top level function filter
      // query, check what will actually be executed. The original query is still fine as padding.
      if (queryFilter.checkBasicReplay(builder.build(), docLookup) != null) {
        replayStats.basicMatchAll.incrementAndGet();
        return originalRequest;
      }
    }
    if (queryTimeoutMs > 0) {
      applyQueryTimeout(builder, queryTimeoutMs, indexState);
    }
    SearchRequest searchRequest = builder.build();
    Runnable replay =
        () ->
            replayQuery(
                indexState,
                searchHandler,
                deadlineScheduler,
                searchRequest,
                warmBasicQuery,
                position,
                replayStats);
    if (threadPoolExecutor == null) {
      replay.run();
    } else {
      threadPoolExecutor.submit(replay);
    }
    return originalRequest;
  }

  /**
   * Limit collection time to the replay timeout, keeping any shorter timeout that would otherwise
   * apply, set on the original query or the index default. Lucene checks this cooperatively while
   * collecting, so the query actually stops instead of continuing to hold a warming thread. The
   * timeout check interval is only set when neither the query nor the index sets one.
   */
  @VisibleForTesting
  static void applyQueryTimeout(
      SearchRequest.Builder builder, int queryTimeoutMs, IndexState indexState) {
    double timeoutSec = queryTimeoutMs / 1000.0;
    double currentTimeoutSec =
        builder.getTimeoutSec() > 0
            ? builder.getTimeoutSec()
            : indexState.getDefaultSearchTimeoutSec();
    if (currentTimeoutSec <= 0 || currentTimeoutSec > timeoutSec) {
      builder.setTimeoutSec(timeoutSec);
    }
    if (builder.getTimeoutCheckEvery() <= 0
        && indexState.getDefaultSearchTimeoutCheckEvery() <= 0) {
      builder.setTimeoutCheckEvery(REPLAY_TIMEOUT_CHECK_EVERY);
    }
  }

  /**
   * Execute a single warming query. A failure or timeout is recorded rather than thrown, so one bad
   * query cannot stop warming or fail startup.
   */
  private void replayQuery(
      IndexState indexState,
      SearchHandler searchHandler,
      ScheduledExecutorService deadlineScheduler,
      SearchRequest searchRequest,
      boolean basicQuery,
      int position,
      ReplayStats replayStats) {
    long startNs = System.nanoTime();
    Reason reason = null;
    try {
      SearchResponse response;
      if (deadlineScheduler == null) {
        response = searchHandler.handle(indexState, searchRequest);
      } else {
        // The request deadline also covers the phases after collection (rescoring, fetching),
        // when deadline cancellation is enabled
        Context.CancellableContext context =
            Context.current()
                .withDeadlineAfter(queryTimeoutMs, TimeUnit.MILLISECONDS, deadlineScheduler);
        try {
          response = context.call(() -> searchHandler.handle(indexState, searchRequest));
        } finally {
          context.cancel(null);
        }
      }
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
      if (queryTimeoutMs > 0
          && (elapsedMs > queryTimeoutMs || (response != null && response.getHitTimeout()))) {
        reason = Reason.TIMEOUT;
      }
    } catch (Exception e) {
      reason = isDeadlineExceeded(e) ? Reason.TIMEOUT : Reason.ERROR;
      if (reason == Reason.ERROR
          && replayStats.failed.incrementAndGet() <= MAX_LOGGED_REPLAY_FAILURES) {
        logger.warn("Warming query failed for index: {}", index, e);
      }
    }
    recordReplayMetrics(basicQuery, reason, System.nanoTime() - startNs);
    if (reason != null) {
      if (reason == Reason.TIMEOUT) {
        replayStats.timedOut.incrementAndGet();
      }
      replayStats.rejectedPositions.add(position);
      queryFilter.recordReplayRejection(reason);
    }
  }

  private void recordReplayMetrics(boolean basicQuery, Reason reason, long elapsedNs) {
    String type = basicQuery ? "basic" : "full";
    String outcome = reason == null ? "success" : (reason == Reason.TIMEOUT ? "timeout" : "error");
    WarmingMetrics.warmingQueryCount.labelValues(index, type, outcome).inc();
    WarmingMetrics.warmingQueryLatency
        .labelValues(index, type)
        .observe(elapsedNs / (double) TimeUnit.SECONDS.toNanos(1));
  }

  /** Deadline cancellation may be wrapped when it happens in a parallel search thread. */
  private static boolean isDeadlineExceeded(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof StatusRuntimeException statusException
          && statusException.getStatus().getCode() == Status.Code.CANCELLED) {
        return true;
      }
    }
    return false;
  }
}
