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

  /** Get the filter used to exclude pathological queries from the warming corpus. */
  public WarmingQueryFilter getQueryFilter() {
    return queryFilter;
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
    List<SearchRequest> observedRequests;
    synchronized (warmingRequests) {
      observedRequests = new ArrayList<>(warmingRequests);
    }
    // Second filter pass, catches queries collected before the filter was configured and
    // pathological queries in the downloaded corpus used for padding
    int numObserved = observedRequests.size();
    observedRequests = filterForBackup(observedRequests);
    int evicted = numObserved - observedRequests.size();
    List<SearchRequest> warmingRequestsToBackup;
    if (padWithDownloadedRequests) {
      List<SearchRequest> downloaded = filterForBackup(this.downloadedWarmingRequests);
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
        "Backed up {} warming queries for index: {} to service: {}, size: {}, evicted by filter: {}",
        count,
        index,
        service,
        warmingQueriesBytes.length,
        evicted);
  }

  private List<SearchRequest> filterForBackup(List<SearchRequest> requests) {
    List<SearchRequest> filtered = new ArrayList<>(requests.size());
    for (SearchRequest request : requests) {
      if (queryFilter.checkBackup(request) == null) {
        filtered.add(request);
      }
    }
    return filtered;
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
   * @param maxDurationMs stop starting new queries after this long, 0 for no limit
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
    if (queryTimeoutMs > 0) {
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
      boolean reachedMaxDuration = false;
      while ((line = reader.readLine()) != null) {
        if (maxDurationMs > 0
            && System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(maxDurationMs)) {
          reachedMaxDuration = true;
          break;
        }
        boolean isStripped = randomThreadLocal.get().nextInt(100) < warmBasicQueryOnlyPerc;
        SearchRequest originalRequest =
            processLine(
                indexState,
                searchHandler,
                threadPoolExecutor,
                deadlineScheduler,
                line,
                isStripped,
                count,
                replayStats);
        if (downloadedQueries != null) {
          downloadedQueries.add(originalRequest);
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
            "Warming index: {} reached max warming duration of {} ms, skipped remaining queries",
            index,
            maxDurationMs);
      }
      logger.info(
          "Warmed index: {} with {} full and {} basic warming queries in {} seconds, timed out: {},"
              + " failed: {}, filtered: {}.",
          index,
          count - basicCount,
          basicCount,
          (System.currentTimeMillis() - startMS) / 1000.0,
          replayStats.timedOut.get(),
          replayStats.failed.get(),
          replayStats.filtered.get());
      if (downloadedQueries != null) {
        // queries that were filtered, timed out or failed during replay are not used to pad the
        // next backup
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
    // position in the downloaded corpus of queries that were filtered, timed out or failed
    final Set<Integer> rejectedPositions = ConcurrentHashMap.newKeySet();
  }

  private SearchRequest processLine(
      IndexState indexState,
      SearchHandler searchHandler,
      ThreadPoolExecutor threadPoolExecutor,
      ScheduledExecutorService deadlineScheduler,
      String line,
      boolean warmBasicQuery,
      int position,
      ReplayStats replayStats)
      throws InvalidProtocolBufferException {
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    JsonFormat.parser().merge(line, builder);
    SearchRequest originalRequest = builder.build();
    // The stored corpus may predate the filter, so check it before replay. A query that fails is
    // also not used to pad the next backup.
    if (queryFilter.checkReplay(originalRequest) != null) {
      replayStats.filtered.incrementAndGet();
      replayStats.rejectedPositions.add(position);
      return originalRequest;
    }
    if (warmBasicQuery) {
      WarmingUtils.simplifySearchRequestForWarming(builder);
      // Simplifying can turn a query into a match all scan, such as a top level function filter
      // query, check what will actually be executed. The original query is still fine as padding.
      if (queryFilter.checkReplay(builder.build()) != null) {
        replayStats.filtered.incrementAndGet();
        return originalRequest;
      }
    }
    if (queryTimeoutMs > 0) {
      applyQueryTimeout(builder, queryTimeoutMs);
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
   * Limit collection time to the replay timeout, keeping any shorter timeout set on the original
   * query. Lucene checks this cooperatively while collecting, so the query actually stops instead
   * of continuing to hold a warming thread.
   */
  @VisibleForTesting
  static void applyQueryTimeout(SearchRequest.Builder builder, int queryTimeoutMs) {
    double timeoutSec = queryTimeoutMs / 1000.0;
    if (builder.getTimeoutSec() <= 0 || builder.getTimeoutSec() > timeoutSec) {
      builder.setTimeoutSec(timeoutSec);
    }
    if (builder.getTimeoutCheckEvery() <= 0) {
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
      if (reason == Reason.ERROR && replayStats.failed.get() < MAX_LOGGED_REPLAY_FAILURES) {
        logger.warn("Warming query failed for index: {}", index, e);
      }
    }
    recordReplayMetrics(basicQuery, reason, System.nanoTime() - startNs);
    if (reason != null) {
      if (reason == Reason.TIMEOUT) {
        replayStats.timedOut.incrementAndGet();
      } else {
        replayStats.failed.incrementAndGet();
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
