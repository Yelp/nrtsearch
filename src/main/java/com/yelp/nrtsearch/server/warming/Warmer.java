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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
  // Wait at least this long for running replay queries, plus the per query timeout if set
  private static final long MIN_REPLAY_WAIT_MS = 10_000;
  // type label of replayed queries generated from the index schema, see warmFromSchema
  static final String SCHEMA_QUERY_TYPE = "schema";
  // type label of replayed queries generated from templates of real queries, see warmFromTemplate
  static final String TEMPLATE_QUERY_TYPE = "template";
  // strategy label values for BootstrapMetrics.warmingStrategyInfo
  static final String S3_STRATEGY = "s3";
  static final String PEER_STRATEGY = "peer";
  // outcome label values for WarmingMetrics.warmingPeerFetchCount
  static final String PEER_FETCH_SUCCESS = "success";
  static final String PEER_FETCH_FALLBACK = "fallback";

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
  private long minReplayWaitMs = MIN_REPLAY_WAIT_MS;
  // TEMPLATE strategy library, null unless enableTemplateLibrary was called
  private volatile WarmerConfig.TemplateLibrarySettings librarySettings;
  private volatile TemplateDriftMonitor driftMonitor;
  private volatile TemplateWarmingQueryGenerator templateLibrary;
  private volatile WarmerConfig.SchemaQuerySettings schemaQuerySettings =
      WarmerConfig.SchemaQuerySettings.DEFAULT;

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
   * Add a query observed in live traffic to the warming corpus, if it is accepted by the query
   * filter and then chosen by reservoir sampling. The filter must run first: a rejected query must
   * not advance the sampler's count, or later accepted queries would be sampled with a lower
   * probability than earlier ones, biasing the corpus toward early traffic. The filter runs its
   * cheap checks first, and only walks the query tree when match all filtering is enabled.
   *
   * @param searchRequest query to add
   * @param latencyMs time taken to execute the query
   * @param docLookup lookup for the index fields, used by the query filter, may be null
   */
  public void maybeAddSearchRequest(
      SearchRequest searchRequest, double latencyMs, DocLookup docLookup) {
    if (queryFilter.checkCollected(searchRequest, latencyMs, docLookup) != null) {
      return;
    }
    TemplateDriftMonitor monitor = driftMonitor;
    if (monitor != null) {
      monitor.observe(searchRequest);
    }
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

  /**
   * Get the queries that make up the current warming corpus: the queries sampled from live traffic,
   * padded with the most recent downloaded queries when padding is enabled and fewer than
   * maxWarmingQueries have been observed. Observed queries were filtered when collected, and
   * downloaded queries when replayed. This is what is backed up to S3 and sent to peers.
   */
  @VisibleForTesting
  List<SearchRequest> getWarmingCorpus() {
    List<SearchRequest> observedRequests;
    synchronized (warmingRequests) {
      observedRequests = new ArrayList<>(warmingRequests);
    }
    if (!padWithDownloadedRequests) {
      return observedRequests;
    }
    List<SearchRequest> downloaded = this.downloadedWarmingRequests;
    int deficit = maxWarmingQueries - observedRequests.size();
    if (deficit <= 0 || downloaded.isEmpty()) {
      return observedRequests;
    }
    int fromIndex = Math.max(0, downloaded.size() - deficit);
    List<SearchRequest> padding = downloaded.subList(fromIndex, downloaded.size());
    List<SearchRequest> corpus = new ArrayList<>(padding.size() + observedRequests.size());
    corpus.addAll(padding);
    corpus.addAll(observedRequests);
    return corpus;
  }

  /** Number of queries in the warming corpus, see {@link #getWarmingCorpus()}. */
  public int getWarmingCorpusSize() {
    return getWarmingCorpus().size();
  }

  /**
   * Serialize the warming corpus as JSON lines, the format stored in S3, in chunks of whole
   * queries. Used to send the corpus to a starting peer replica.
   *
   * @param maxQueries maximum number of queries to write, 0 for no limit
   * @param maxBytes maximum total bytes to write, 0 for no limit. Writing stops before the first
   *     query that would exceed it.
   * @param chunkBytes target chunk size, a chunk is emitted once it reaches this size. A single
   *     query larger than this is emitted as its own chunk.
   * @param chunkConsumer receives each chunk
   * @return number of queries written
   */
  public int writeWarmingCorpus(
      int maxQueries, long maxBytes, int chunkBytes, Consumer<byte[]> chunkConsumer) {
    List<SearchRequest> corpus = getWarmingCorpus();
    ByteArrayOutputStream chunk = new ByteArrayOutputStream(chunkBytes);
    JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    long totalBytes = 0;
    int count = 0;
    for (SearchRequest searchRequest : corpus) {
      if (maxQueries > 0 && count >= maxQueries) {
        break;
      }
      byte[] line;
      try {
        line = (printer.print(searchRequest) + "\n").getBytes(StandardCharsets.UTF_8);
      } catch (InvalidProtocolBufferException e) {
        logger.warn("Unable to serialize warming query for index: {}", index, e);
        continue;
      }
      if (maxBytes > 0 && totalBytes + line.length > maxBytes) {
        break;
      }
      if (chunk.size() > 0 && chunk.size() + line.length > chunkBytes) {
        chunkConsumer.accept(chunk.toByteArray());
        chunk.reset();
      }
      chunk.write(line, 0, line.length);
      totalBytes += line.length;
      count++;
    }
    if (chunk.size() > 0) {
      chunkConsumer.accept(chunk.toByteArray());
    }
    return count;
  }

  public synchronized void backupWarmingQueriesToS3(String service) throws IOException {
    if (Strings.isNullOrEmpty(service)) {
      service = this.service;
    }

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    int count = 0;
    List<SearchRequest> warmingRequestsToBackup = getWarmingCorpus();
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
    warmFromS3(indexState, parallelism, 0, new AtomicBoolean());
  }

  /**
   * Replay the warming queries stored in S3.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param maxDurationMs stop starting new queries after this long, 0 for no limit. The remaining
   *     stored queries are still read, so they can pad the next backup.
   * @param stopRequested when set, stop starting new queries in the same way as reaching the max
   *     duration. Lets a caller that stopped waiting for warming stop new queries from starting,
   *     since this method's own clock starts later than the caller's. The flag is checked before
   *     each stored query, so a query that passed the check just before it was set may still start.
   */
  public void warmFromS3(
      IndexState indexState, int parallelism, long maxDurationMs, AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    try (Timer _timer =
        BootstrapMetrics.warmingQueryTimer.labelValues(service, index).startTimer()) {
      SearchHandler searchHandler =
          new SearchHandler(indexState.getGlobalState(), indexState.getSearchExecutor(), true);
      warmFromS3(indexState, parallelism, searchHandler, maxDurationMs, stopRequested);
    }
  }

  @VisibleForTesting
  void warmFromS3(IndexState indexState, int parallelism, SearchHandler searchHandler)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    warmFromS3(indexState, parallelism, searchHandler, 0, new AtomicBoolean());
  }

  @VisibleForTesting
  void warmFromS3(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    if (!remoteBackend.exists(service, index, IndexResourceType.WARMING_QUERIES)) {
      logger.info("No warming queries found in S3 for service: {} and index: {}", service, index);
      return;
    }
    replayStoredQueries(
        indexState,
        parallelism,
        searchHandler,
        maxDurationMs,
        stopRequested,
        remoteBackend.downloadWarmingQueries(service, index),
        S3_STRATEGY);
  }

  /**
   * Replay stored warming queries, one SearchRequest JSON per line, read from the given stream. The
   * stream is closed when done. Used for queries downloaded from S3 and fetched from a peer.
   *
   * @param strategy warming strategy label for the warming strategy metric
   */
  private void replayStoredQueries(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      long maxDurationMs,
      AtomicBoolean stopRequested,
      InputStream queries,
      String strategy)
      throws IOException, InterruptedException {
    BootstrapMetrics.warmingStrategyInfo.labelValues(index, strategy).set(1);
    ThreadPoolExecutor threadPoolExecutor = createWarmingExecutor(parallelism);
    ScheduledExecutorService deadlineScheduler = createDeadlineScheduler();
    long startMS = System.currentTimeMillis();
    long startNs = System.nanoTime();
    List<SearchRequest> downloadedQueries = padWithDownloadedRequests ? new ArrayList<>() : null;
    ReplayStats replayStats = new ReplayStats();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(queries, StateUtils.getValidatingUTF8Decoder()))) {
      String line;
      int count = 0, basicCount = 0;
      int skipped = 0;
      boolean reachedMaxDuration = false;
      // whether the caller stopped waiting, rather than this method's own max duration
      boolean stoppedByCaller = false;
      while ((line = reader.readLine()) != null) {
        if (!reachedMaxDuration) {
          if (stopRequested.get()) {
            reachedMaxDuration = true;
            stoppedByCaller = true;
          } else if (maxDurationMs > 0
              && System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(maxDurationMs)) {
            reachedMaxDuration = true;
          }
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
      if (!shutdownAndWait(threadPoolExecutor)) {
        logger.warn(
            "Warming index: {} still has {} warming queries running after waiting {} ms, they"
                + " continue in the background and are not used for padding",
            index,
            replayStats.inFlightPositions.size(),
            replayWaitMs());
      }
      if (stoppedByCaller) {
        logger.warn(
            "Warming index: {} stopped starting new queries since startup stopped waiting,"
                + " skipped replaying {} remaining queries",
            index,
            skipped);
      } else if (reachedMaxDuration) {
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
        // queries that timed out, failed, or are still running (so their result is unknown) are
        // not used to pad the next backup, filtered and unparsable queries were never added
        Set<Integer> inFlight = Set.copyOf(replayStats.inFlightPositions);
        List<SearchRequest> replayable = new ArrayList<>(downloadedQueries.size());
        for (int i = 0; i < downloadedQueries.size(); ++i) {
          if (!replayStats.rejectedPositions.contains(i) && !inFlight.contains(i)) {
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

  /**
   * Stop accepting replay tasks and wait for running ones. Does nothing if already shut down, so
   * the wait is not repeated in the finally block.
   *
   * @return true if all replay tasks finished
   */
  private boolean shutdownAndWait(ThreadPoolExecutor threadPoolExecutor)
      throws InterruptedException {
    return shutdownAndWait(threadPoolExecutor, queryTimeoutMs);
  }

  private boolean shutdownAndWait(ThreadPoolExecutor threadPoolExecutor, int timeoutMs)
      throws InterruptedException {
    if (threadPoolExecutor == null || threadPoolExecutor.isShutdown()) {
      return true;
    }
    threadPoolExecutor.shutdown();
    return threadPoolExecutor.awaitTermination(replayWaitMs(timeoutMs), TimeUnit.MILLISECONDS);
  }

  /** Time to wait for running replay tasks, long enough for a query to reach its timeout. */
  private long replayWaitMs() {
    return replayWaitMs(queryTimeoutMs);
  }

  private long replayWaitMs(int timeoutMs) {
    return minReplayWaitMs + Math.max(0, timeoutMs);
  }

  @VisibleForTesting
  void setMinReplayWaitMs(long minReplayWaitMs) {
    this.minReplayWaitMs = minReplayWaitMs;
  }

  /** Outcome of replayed queries, updated concurrently by warming threads. */
  private static class ReplayStats {
    final AtomicInteger timedOut = new AtomicInteger();
    final AtomicInteger failed = new AtomicInteger();
    final AtomicInteger filtered = new AtomicInteger();
    final AtomicInteger basicMatchAll = new AtomicInteger();
    // position in the downloaded queries of queries that timed out or failed
    final Set<Integer> rejectedPositions = ConcurrentHashMap.newKeySet();
    // position of queries that have been started and not yet finished
    final Set<Integer> inFlightPositions = ConcurrentHashMap.newKeySet();
  }

  /**
   * Warm the index with queries generated from its schema and values, see {@link
   * SchemaWarmingQueryGenerator}. Nothing is downloaded from S3. If no queries can be generated,
   * for example when the index has no searchable fields, falls back to {@link
   * #warmFromS3(IndexState, int)}.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param numQueries number of term and range queries to generate
   */
  public void warmFromSchema(IndexState indexState, int parallelism, int numQueries)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    warmFromSchema(indexState, parallelism, numQueries, 0, new AtomicBoolean());
  }

  /**
   * Warm the index with queries generated from its schema and values. Generated queries are
   * replayed like stored ones: each uses the per query timeout, failures and timeouts are recorded
   * rather than thrown, and no new query starts after the max duration or once stop is requested.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param numQueries number of term and range queries to generate
   * @param maxDurationMs stop starting new queries after this long, including the time to generate
   *     them, 0 for no limit. Also applies to the S3 fallback.
   * @param stopRequested when set, stop starting new queries, see {@link #warmFromS3(IndexState,
   *     int, long, AtomicBoolean)}
   */
  public void warmFromSchema(
      IndexState indexState,
      int parallelism,
      int numQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    try (Timer _timer =
        BootstrapMetrics.warmingQueryTimer.labelValues(service, index).startTimer()) {
      SearchHandler searchHandler =
          new SearchHandler(indexState.getGlobalState(), indexState.getSearchExecutor(), true);
      warmFromSchema(
          indexState, parallelism, searchHandler, numQueries, maxDurationMs, stopRequested);
    }
  }

  private void warmFromSchema(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      int numQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    long startNs = System.nanoTime();
    List<SearchRequest> requests;
    try {
      requests = generateSchemaQueries(indexState, numQueries);
    } catch (IOException | RuntimeException e) {
      logger.warn(
          "Failed to generate schema warming queries for index: {}, falling back to S3", index, e);
      requests = List.of();
    }
    if (requests.isEmpty()) {
      logger.info("No schema warming queries for index: {}, falling back to S3", index);
      warmFromS3(
          indexState,
          parallelism,
          searchHandler,
          remainingMs(maxDurationMs, startNs),
          stopRequested);
      return;
    }
    warmFromRequests(
        indexState,
        parallelism,
        searchHandler,
        requests,
        maxDurationMs,
        stopRequested,
        startNs,
        SCHEMA_QUERY_TYPE);
  }

  @VisibleForTesting
  List<SearchRequest> generateSchemaQueries(IndexState indexState, int numQueries)
      throws IOException {
    return new SchemaWarmingQueryGenerator(
            indexState, queryFilter.getMaxQueryTopHits(), schemaQuerySettings.limits())
        .generate(numQueries);
  }

  /** Set how schema generated queries are generated and run, see {@link WarmerConfig}. */
  public void setSchemaQuerySettings(WarmerConfig.SchemaQuerySettings settings) {
    this.schemaQuerySettings = settings;
  }

  /**
   * Warm the index with queries generated from the templates of a few real queries, see {@link
   * TemplateWarmingQueryGenerator}. The seed queries come from a running peer replica, or from the
   * S3 corpus if no peer provides them. The seeds are run first, with their retrieve fields
   * replaced by the template fields, to build the value pool from the documents real queries
   * return. Then numQueries generated queries are replayed like schema queries. Falls back to
   * {@link WarmerConfig.WarmingStrategy#SCHEMA} if there are no seeds or no queries can be
   * generated.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param peerSource source of seed queries from a peer
   * @param numSeeds number of real queries to learn templates from
   * @param numQueries number of queries to generate
   * @param maxDurationMs stop starting new queries after this long, including fetching seeds and
   *     building the value pool, 0 for no limit
   * @param stopRequested when set, stop starting new queries, see {@link #warmFromS3(IndexState,
   *     int, long, AtomicBoolean)}
   */
  public void warmFromTemplate(
      IndexState indexState,
      int parallelism,
      PeerWarmingQuerySource peerSource,
      int numSeeds,
      int numQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    warmFromTemplate(
        indexState, parallelism, peerSource, numSeeds, numQueries, 0, maxDurationMs, stopRequested);
  }

  /**
   * Same as {@link #warmFromTemplate(IndexState, int, PeerWarmingQuerySource, int, int, long,
   * AtomicBoolean)}, then replay numSchemaQueries schema generated queries in the time left.
   *
   * @param numSchemaQueries schema generated queries to replay after the template queries, 0 for
   *     none. Not replayed when template warming fell back to SCHEMA.
   */
  public void warmFromTemplate(
      IndexState indexState,
      int parallelism,
      PeerWarmingQuerySource peerSource,
      int numSeeds,
      int numQueries,
      int numSchemaQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    try (Timer _timer =
        BootstrapMetrics.warmingQueryTimer.labelValues(service, index).startTimer()) {
      SearchHandler searchHandler =
          new SearchHandler(indexState.getGlobalState(), indexState.getSearchExecutor(), true);
      warmFromTemplate(
          indexState,
          parallelism,
          searchHandler,
          peerSource,
          numSeeds,
          numQueries,
          numSchemaQueries,
          maxDurationMs,
          stopRequested);
    }
  }

  @VisibleForTesting
  void warmFromTemplate(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      PeerWarmingQuerySource peerSource,
      int numSeeds,
      int numQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    warmFromTemplate(
        indexState,
        parallelism,
        searchHandler,
        peerSource,
        numSeeds,
        numQueries,
        0,
        maxDurationMs,
        stopRequested);
  }

  @VisibleForTesting
  void warmFromTemplate(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      PeerWarmingQuerySource peerSource,
      int numSeeds,
      int numQueries,
      int numSchemaQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    long startNs = System.nanoTime();
    DocLookup docLookup = indexState.docLookup;
    boolean useLibrary = librarySettings != null && librarySettings.enabled();
    TemplateWarmingQueryGenerator generator = null;
    // real queries run to build the value pool: the seeds, or library queries with real values
    List<SearchRequest> poolQueries = List.of();
    String source = "none";
    if (useLibrary) {
      generator = loadTemplateLibrary();
      if (generator != null && generator.getNumTemplates() > 0) {
        source = "library";
        poolQueries = filtered(generator.generate(numSeeds), docLookup);
      } else {
        generator = null;
      }
    }
    if (generator == null) {
      // seeds and generated queries pass the same guardrails as replayed stored queries; the S3
      // corpus may predate them
      List<SearchRequest> seeds = filtered(fetchTemplateSeeds(peerSource, numSeeds), docLookup);
      if (!seeds.isEmpty()) {
        generator = new TemplateWarmingQueryGenerator(seeds, new Random());
        poolQueries = seeds;
        source = "learned";
        if (useLibrary && generator.getNumTemplates() > 0) {
          // learn once: later startups load this library instead of learning from seeds
          storeTemplateLibrary(generator);
        }
      }
    }
    WarmingMetrics.warmingTemplateLibraryCount.labelValues(index, source).inc();
    if (useLibrary) {
      setLibrary(generator);
    }
    List<SearchRequest> requests = List.of();
    if (generator != null && generator.getNumTemplates() > 0) {
      try {
        if (!poolQueries.isEmpty()) {
          buildTemplateValuePool(indexState, searchHandler, generator, poolQueries, stopRequested);
        }
        requests = filtered(generator.generate(numQueries), docLookup);
      } catch (RuntimeException e) {
        logger.warn("Failed to generate template warming queries for index: {}", index, e);
        requests = List.of();
      }
    }
    if (requests.isEmpty()) {
      logger.info("No template warming queries for index: {}, falling back to SCHEMA", index);
      warmFromSchema(
          indexState,
          parallelism,
          searchHandler,
          numQueries,
          remainingMs(maxDurationMs, startNs),
          stopRequested);
      return;
    }
    warmFromRequests(
        indexState,
        parallelism,
        searchHandler,
        requests,
        maxDurationMs,
        stopRequested,
        startNs,
        TEMPLATE_QUERY_TYPE);
    if (numSchemaQueries > 0) {
      warmFromSchemaAfterTemplate(
          indexState,
          parallelism,
          searchHandler,
          numSchemaQueries,
          maxDurationMs,
          stopRequested,
          startNs);
    }
  }

  /**
   * Replay schema generated queries after template queries, sharing the max duration. Unlike {@link
   * WarmerConfig.WarmingStrategy#SCHEMA}, nothing falls back to S3 if no schema query can be
   * generated, since the index is already warmed by the template queries.
   */
  private void warmFromSchemaAfterTemplate(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      int numSchemaQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested,
      long startNs)
      throws InterruptedException {
    if (stopRequested.get()
        || (maxDurationMs > 0
            && System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(maxDurationMs))) {
      logger.info(
          "No time left for schema warming queries after template queries for index: {}", index);
      return;
    }
    List<SearchRequest> schemaRequests;
    try {
      schemaRequests =
          filtered(generateSchemaQueries(indexState, numSchemaQueries), indexState.docLookup);
    } catch (IOException | RuntimeException e) {
      logger.warn(
          "Failed to generate schema warming queries after template queries for index: {}",
          index,
          e);
      return;
    }
    if (schemaRequests.isEmpty()) {
      logger.info("No schema warming queries after template queries for index: {}", index);
      return;
    }
    warmFromRequests(
        indexState,
        parallelism,
        searchHandler,
        schemaRequests,
        maxDurationMs,
        stopRequested,
        startNs,
        SCHEMA_QUERY_TYPE);
  }

  private List<SearchRequest> filtered(List<SearchRequest> requests, DocLookup docLookup) {
    List<SearchRequest> out = new ArrayList<>(requests.size());
    for (SearchRequest r : requests) {
      if (queryFilter.checkReplay(r, docLookup) == null) {
        out.add(r);
      }
    }
    return out;
  }

  // ------------------------------------------------------------------ template library

  /**
   * Store learned templates in the remote backend, load them on startup, and relearn them from
   * collected live queries when those stop matching. Only used by the TEMPLATE strategy.
   */
  public void enableTemplateLibrary(WarmerConfig.TemplateLibrarySettings settings) {
    enableTemplateLibrary(
        settings,
        new TemplateDriftMonitor(
            index,
            settings.driftCheckEvery(),
            settings.driftWindow(),
            settings.relearnMatchThreshold(),
            TimeUnit.MINUTES.toMillis(settings.relearnCooldownMinutes()),
            this::relearnTemplates));
  }

  @VisibleForTesting
  void enableTemplateLibrary(
      WarmerConfig.TemplateLibrarySettings settings, TemplateDriftMonitor monitor) {
    this.librarySettings = settings;
    this.driftMonitor = monitor;
  }

  @VisibleForTesting
  TemplateWarmingQueryGenerator getTemplateLibrary() {
    return templateLibrary;
  }

  @VisibleForTesting
  TemplateDriftMonitor getDriftMonitor() {
    return driftMonitor;
  }

  /**
   * Use the given templates as the library, and check live queries against them. With no templates,
   * every check misses, so the first full check window triggers learning from the collected live
   * queries.
   */
  private void setLibrary(TemplateWarmingQueryGenerator library) {
    templateLibrary = library;
    if (driftMonitor != null) {
      driftMonitor.setLibraryKeys(library == null ? Set.of() : library.templateKeys());
    }
  }

  private TemplateWarmingQueryGenerator loadTemplateLibrary() {
    try {
      if (!remoteBackend.exists(service, index, IndexResourceType.WARMING_TEMPLATES)) {
        logger.info("No template warming library for index: {}", index);
        return null;
      }
      try (InputStream in = remoteBackend.downloadWarmingTemplates(service, index)) {
        TemplateWarmingQueryGenerator library =
            TemplateWarmingQueryGenerator.fromLibrary(
                new String(in.readAllBytes(), StandardCharsets.UTF_8), new Random());
        logger.info(
            "Loaded template warming library for index: {} with {} templates",
            index,
            library.getNumTemplates());
        return library;
      }
    } catch (IOException | RuntimeException e) {
      logger.warn("Failed to load the template warming library for index: {}", index, e);
      return null;
    }
  }

  private boolean storeTemplateLibrary(TemplateWarmingQueryGenerator library) {
    try {
      byte[] data = library.toLibrary().getBytes(StandardCharsets.UTF_8);
      remoteBackend.uploadWarmingTemplates(service, index, data);
      logger.info(
          "Stored template warming library for index: {} with {} templates, size: {}",
          index,
          library.getNumTemplates(),
          data.length);
      return true;
    } catch (IOException | RuntimeException e) {
      logger.warn("Failed to store the template warming library for index: {}", index, e);
      return false;
    }
  }

  /**
   * Learn templates from the collected live queries, merge them into the library and store it.
   * Called by the drift monitor on a background thread when the match ratio drops.
   *
   * @return keys of the new library, or null if there are too few collected queries
   */
  @VisibleForTesting
  Set<String> relearnTemplates(double matchRatio) {
    List<SearchRequest> corpus;
    synchronized (warmingRequests) {
      corpus = new ArrayList<>(warmingRequests);
    }
    if (corpus.size() < librarySettings.relearnMinQueries()) {
      logger.info(
          "Not relearning templates for index: {}, only {} collected queries",
          index,
          corpus.size());
      return null;
    }
    TemplateWarmingQueryGenerator learned = new TemplateWarmingQueryGenerator(corpus, new Random());
    TemplateWarmingQueryGenerator current = templateLibrary;
    TemplateWarmingQueryGenerator merged =
        current == null
            ? learned
            : current.merge(learned, librarySettings.decay(), librarySettings.minWeight());
    if (!storeTemplateLibrary(merged)) {
      throw new IllegalStateException("Unable to store the relearned template library");
    }
    templateLibrary = merged;
    logger.info(
        "Relearned templates for index: {} after match ratio {}: {} templates learned from {}"
            + " queries, library now has {}",
        index,
        matchRatio,
        learned.getNumTemplates(),
        corpus.size(),
        merged.getNumTemplates());
    return merged.templateKeys();
  }

  /** Up to numSeeds parsable queries from a peer's corpus, else from the S3 corpus. */
  private List<SearchRequest> fetchTemplateSeeds(PeerWarmingQuerySource peerSource, int numSeeds)
      throws IOException {
    List<SearchRequest> seeds = List.of();
    if (peerSource != null) {
      try {
        Optional<PeerWarmingQuerySource.PeerWarmingQueries> fetched = peerSource.fetch();
        if (fetched.isPresent()) {
          seeds = readSeeds(new ByteArrayInputStream(fetched.get().content()), numSeeds);
          logger.info(
              "Template warming index: {} with {} seed queries from peer: {}",
              index,
              seeds.size(),
              fetched.get().peer());
        }
      } catch (RuntimeException e) {
        logger.warn("Failed to fetch template seed queries from a peer for index: {}", index, e);
      }
    }
    if (seeds.isEmpty()
        && remoteBackend.exists(service, index, IndexResourceType.WARMING_QUERIES)) {
      seeds = readSeeds(remoteBackend.downloadWarmingQueries(service, index), numSeeds);
      logger.info("Template warming index: {} with {} seed queries from S3", index, seeds.size());
    }
    return seeds;
  }

  private List<SearchRequest> readSeeds(InputStream queries, int numSeeds) throws IOException {
    List<SearchRequest> seeds = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(queries, StateUtils.getValidatingUTF8Decoder()))) {
      String line;
      while (seeds.size() < numSeeds && (line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        SearchRequest.Builder builder = SearchRequest.newBuilder();
        try {
          JsonFormat.parser().ignoringUnknownFields().merge(line, builder);
          seeds.add(builder.build());
        } catch (InvalidProtocolBufferException e) {
          // skip, the corpus may contain a query this version can't parse
        }
      }
    }
    return seeds;
  }

  /**
   * Run the seed queries with the template fields as retrieve fields, and add the returned
   * documents to the generator's value pool. Fields that can't be retrieved, such as fields of
   * nested documents, are dropped first, so they don't fail every pool query.
   */
  private void buildTemplateValuePool(
      IndexState indexState,
      SearchHandler searchHandler,
      TemplateWarmingQueryGenerator generator,
      List<SearchRequest> seeds,
      AtomicBoolean stopRequested) {
    List<String> fields = new ArrayList<>(generator.fields());
    if (fields.isEmpty()) {
      return;
    }
    SearchRequest probe = seeds.get(0);
    if (!retrieves(indexState, searchHandler, probe, fields)) {
      List<String> ok = new ArrayList<>();
      for (String field : fields) {
        if (retrieves(indexState, searchHandler, probe, List.of(field))) {
          ok.add(field);
        }
      }
      fields = ok;
    }
    if (fields.isEmpty()) {
      return;
    }
    long startNs = System.nanoTime();
    int failed = 0;
    for (SearchRequest seed : seeds) {
      if (stopRequested.get()) {
        break;
      }
      try {
        generator.addPoolHits(
            searchHandler.handle(indexState, withRetrieveFields(seed, fields, indexState)));
      } catch (RuntimeException | SearchHandler.SearchHandlerException e) {
        failed++;
      }
    }
    logger.info(
        "Template warming value pool for index: {}: {} documents from {} seed queries ({} failed)"
            + " and {} fields in {} ms",
        index,
        generator.getPoolSize(),
        seeds.size(),
        failed,
        fields.size(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
  }

  private boolean retrieves(
      IndexState indexState, SearchHandler searchHandler, SearchRequest seed, List<String> fields) {
    try {
      searchHandler.handle(indexState, withRetrieveFields(seed, fields, indexState));
      return true;
    } catch (RuntimeException | SearchHandler.SearchHandlerException e) {
      return false;
    }
  }

  private SearchRequest withRetrieveFields(
      SearchRequest seed, List<String> fields, IndexState indexState) {
    SearchRequest.Builder builder =
        seed.toBuilder().clearRetrieveFields().addAllRetrieveFields(fields);
    if (queryTimeoutMs > 0) {
      applyQueryTimeout(builder, queryTimeoutMs, indexState);
    }
    return builder.build();
  }

  /**
   * Warm the index with the warming corpus of a running peer replica, see {@link
   * PeerWarmingQuerySource}. The peer's queries are fresh samples of live traffic, and are replayed
   * the same way as queries stored in S3: filtered, with the per query timeout, and padding the
   * next backup when enabled. If no peer can provide queries, warms with the fallback strategy.
   *
   * @param indexState index to warm
   * @param parallelism number of queries to run in parallel
   * @param peerSource source of the peer's warming corpus
   * @param fallbackStrategy strategy used if no peer provides queries: S3, SCHEMA or NONE
   * @param schemaQueries number of queries to generate with the SCHEMA fallback
   * @param maxDurationMs stop starting new queries after this long, including the time to fetch
   *     from the peer and any fallback, 0 for no limit
   * @param stopRequested when set, stop starting new queries, see {@link #warmFromS3(IndexState,
   *     int, long, AtomicBoolean)}
   */
  public void warmFromPeer(
      IndexState indexState,
      int parallelism,
      PeerWarmingQuerySource peerSource,
      WarmerConfig.WarmingStrategy fallbackStrategy,
      int schemaQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    try (Timer _timer =
        BootstrapMetrics.warmingQueryTimer.labelValues(service, index).startTimer()) {
      SearchHandler searchHandler =
          new SearchHandler(indexState.getGlobalState(), indexState.getSearchExecutor(), true);
      warmFromPeer(
          indexState,
          parallelism,
          searchHandler,
          peerSource,
          fallbackStrategy,
          schemaQueries,
          maxDurationMs,
          stopRequested);
    }
  }

  @VisibleForTesting
  void warmFromPeer(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      PeerWarmingQuerySource peerSource,
      WarmerConfig.WarmingStrategy fallbackStrategy,
      int schemaQueries,
      long maxDurationMs,
      AtomicBoolean stopRequested)
      throws IOException, SearchHandler.SearchHandlerException, InterruptedException {
    long startNs = System.nanoTime();
    Optional<PeerWarmingQuerySource.PeerWarmingQueries> peerQueries;
    try {
      peerQueries = peerSource.fetch();
    } catch (RuntimeException e) {
      logger.warn("Failed to fetch warming queries from a peer for index: {}", index, e);
      peerQueries = Optional.empty();
    }
    if (peerQueries.isPresent()) {
      PeerWarmingQuerySource.PeerWarmingQueries fetched = peerQueries.get();
      WarmingMetrics.warmingPeerFetchCount.labelValues(index, PEER_FETCH_SUCCESS).inc();
      logger.info(
          "Warming index: {} with {} bytes of warming queries from peer: {}, fetched in {} ms",
          index,
          fetched.content().length,
          fetched.peer(),
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
      replayStoredQueries(
          indexState,
          parallelism,
          searchHandler,
          remainingMs(maxDurationMs, startNs),
          stopRequested,
          new ByteArrayInputStream(fetched.content()),
          PEER_STRATEGY);
      return;
    }
    WarmingMetrics.warmingPeerFetchCount.labelValues(index, PEER_FETCH_FALLBACK).inc();
    logger.info(
        "No peer provided warming queries for index: {}, falling back to: {}",
        index,
        fallbackStrategy);
    long remainingMs = remainingMs(maxDurationMs, startNs);
    switch (fallbackStrategy) {
      case S3 -> warmFromS3(indexState, parallelism, searchHandler, remainingMs, stopRequested);
      case SCHEMA ->
          warmFromSchema(
              indexState, parallelism, searchHandler, schemaQueries, remainingMs, stopRequested);
      case NONE -> BootstrapMetrics.warmingStrategyInfo.labelValues(index, "none").set(1);
      case PEER, TEMPLATE ->
          throw new IllegalArgumentException(
              "Peer warming fallback strategy cannot be " + fallbackStrategy);
    }
  }

  /** Time left of the max duration since startNs, 0 when there is no max duration. */
  private static long remainingMs(long maxDurationMs, long startNs) {
    if (maxDurationMs <= 0) {
      return 0;
    }
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    // at least 1, since 0 would mean no limit
    return Math.max(1, maxDurationMs - elapsedMs);
  }

  /**
   * Run the given warming queries. A failing query is logged and skipped, so one bad generated
   * query cannot fail startup.
   */
  @VisibleForTesting
  void warmFromRequests(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      List<SearchRequest> requests)
      throws InterruptedException {
    warmFromRequests(
        indexState,
        parallelism,
        searchHandler,
        requests,
        0,
        new AtomicBoolean(),
        System.nanoTime(),
        SCHEMA_QUERY_TYPE);
  }

  /**
   * @param queryType label for the warming strategy and query metrics: schema or template
   */
  private void warmFromRequests(
      IndexState indexState,
      int parallelism,
      SearchHandler searchHandler,
      List<SearchRequest> requests,
      long maxDurationMs,
      AtomicBoolean stopRequested,
      long startNs,
      String queryType)
      throws InterruptedException {
    BootstrapMetrics.warmingStrategyInfo.labelValues(index, queryType).set(1);
    boolean isSchema = SCHEMA_QUERY_TYPE.equals(queryType);
    WarmerConfig.SchemaQuerySettings settings = schemaQuerySettings;
    if (isSchema && settings.parallelism() > 0) {
      parallelism = settings.parallelism();
    }
    int timeoutMs =
        isSchema && settings.queryTimeoutMs() > 0 ? settings.queryTimeoutMs() : queryTimeoutMs;
    ThreadPoolExecutor threadPoolExecutor = createWarmingExecutor(parallelism);
    ScheduledExecutorService deadlineScheduler = createDeadlineScheduler(timeoutMs);
    long startMS = System.currentTimeMillis();
    ReplayStats replayStats = new ReplayStats();
    Map<String, KindStats> kindStats = isSchema ? new ConcurrentHashMap<>() : null;
    int started = 0;
    boolean stoppedByCaller = false;
    boolean reachedMaxDuration = false;
    try {
      for (SearchRequest request : requests) {
        if (stopRequested.get()) {
          stoppedByCaller = true;
          break;
        }
        if (maxDurationMs > 0
            && System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(maxDurationMs)) {
          reachedMaxDuration = true;
          break;
        }
        SearchRequest.Builder builder = request.toBuilder();
        if (timeoutMs > 0) {
          applyQueryTimeout(builder, timeoutMs, indexState);
        }
        SearchRequest searchRequest = builder.build();
        String kind = isSchema ? SchemaWarmingQueryGenerator.kindOf(request) : null;
        int position = started++;
        replayStats.inFlightPositions.add(position);
        Runnable replay =
            () ->
                replayQuery(
                    indexState,
                    searchHandler,
                    deadlineScheduler,
                    searchRequest,
                    queryType,
                    position,
                    replayStats,
                    timeoutMs,
                    kind,
                    kindStats);
        if (threadPoolExecutor == null) {
          replay.run();
        } else {
          threadPoolExecutor.submit(replay);
        }
      }
      if (!shutdownAndWait(threadPoolExecutor, timeoutMs)) {
        logger.warn(
            "Warming index: {} still has {} {} warming queries running after waiting {} ms,"
                + " they continue in the background",
            index,
            replayStats.inFlightPositions.size(),
            queryType,
            replayWaitMs(timeoutMs));
      }
    } finally {
      shutdownAndWait(threadPoolExecutor, timeoutMs);
      if (deadlineScheduler != null) {
        deadlineScheduler.shutdownNow();
      }
    }
    int skipped = requests.size() - started;
    if (stoppedByCaller) {
      logger.warn(
          "Warming index: {} stopped starting new queries since startup stopped waiting, skipped"
              + " {} remaining {} warming queries",
          index,
          skipped,
          queryType);
    } else if (reachedMaxDuration) {
      logger.warn(
          "Warming index: {} reached max warming duration of {} ms, skipped {} remaining {}"
              + " warming queries",
          index,
          maxDurationMs,
          skipped,
          queryType);
    }
    logger.info(
        "Warmed index: {} with {} {} warming queries in {} seconds, timed out: {}, failed: {}.",
        index,
        started,
        queryType,
        (System.currentTimeMillis() - startMS) / 1000.0,
        replayStats.timedOut.get(),
        replayStats.failed.get());
    if (kindStats != null && !kindStats.isEmpty()) {
      logger.info(
          "Schema warming queries of index: {} by kind (count, total s, mean ms, max ms, timed"
              + " out), parallelism {}: {}",
          index,
          parallelism,
          formatKindStats(kindStats));
    }
  }

  /** Cost of the replayed schema queries of one kind, updated concurrently by warming threads. */
  @VisibleForTesting
  static class KindStats {
    final java.util.concurrent.atomic.LongAdder count = new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.LongAdder totalNs =
        new java.util.concurrent.atomic.LongAdder();
    final java.util.concurrent.atomic.AtomicLong maxNs =
        new java.util.concurrent.atomic.AtomicLong();
    final AtomicInteger timedOut = new AtomicInteger();

    void record(long elapsedNs, boolean timeout) {
      count.increment();
      totalNs.add(elapsedNs);
      maxNs.accumulateAndGet(elapsedNs, Math::max);
      if (timeout) {
        timedOut.incrementAndGet();
      }
    }
  }

  /** Kinds by total time, most costly first, e.g. "script=111/150.2s/1353ms/9876ms/7". */
  @VisibleForTesting
  static String formatKindStats(Map<String, KindStats> kindStats) {
    return kindStats.entrySet().stream()
        .sorted((a, b) -> Long.compare(b.getValue().totalNs.sum(), a.getValue().totalNs.sum()))
        .map(
            e -> {
              KindStats s = e.getValue();
              long count = s.count.sum();
              double totalMs = s.totalNs.sum() / 1e6;
              return String.format(
                  java.util.Locale.ROOT,
                  "%s=%d/%.1fs/%.0fms/%.0fms/%d",
                  e.getKey(),
                  count,
                  totalMs / 1000,
                  count == 0 ? 0 : totalMs / count,
                  s.maxNs.get() / 1e6,
                  s.timedOut.get());
            })
        .collect(java.util.stream.Collectors.joining(", "));
  }

  private static ThreadPoolExecutor createWarmingExecutor(int parallelism) {
    if (parallelism <= 1) {
      return null;
    }
    int numThreads = parallelism - 1;
    return new ThreadPoolExecutor(
        numThreads,
        numThreads,
        0,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new NamedThreadFactory("warming-"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  /** Scheduler for replay deadlines, null when there is no per query timeout to enforce. */
  private ScheduledExecutorService createDeadlineScheduler() {
    return createDeadlineScheduler(queryTimeoutMs);
  }

  private ScheduledExecutorService createDeadlineScheduler(int timeoutMs) {
    // the request deadline only has an effect when deadline cancellation is enabled
    if (timeoutMs > 0 && DeadlineUtils.getCancellationEnabled()) {
      return Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("warming-deadline-%d").setDaemon(true).build());
    }
    return null;
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
      // not counted in warmingQueryCount, which only counts replayed queries
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
    replayStats.inFlightPositions.add(position);
    Runnable replay =
        () ->
            replayQuery(
                indexState,
                searchHandler,
                deadlineScheduler,
                searchRequest,
                warmBasicQuery ? "basic" : "full",
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
      String type,
      int position,
      ReplayStats replayStats) {
    replayQuery(
        indexState,
        searchHandler,
        deadlineScheduler,
        searchRequest,
        type,
        position,
        replayStats,
        queryTimeoutMs,
        null,
        null);
  }

  /**
   * @param timeoutMs timeout of this query, 0 for none
   * @param kind kind of a schema generated query, see {@link SchemaWarmingQueryGenerator#kindOf},
   *     null for other queries
   * @param kindStats per kind costs to update, null when kind is null
   */
  private void replayQuery(
      IndexState indexState,
      SearchHandler searchHandler,
      ScheduledExecutorService deadlineScheduler,
      SearchRequest searchRequest,
      String type,
      int position,
      ReplayStats replayStats,
      int queryTimeoutMs,
      String kind,
      Map<String, KindStats> kindStats) {
    long startNs = System.nanoTime();
    Reason reason = null;
    try {
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
      long elapsedNs = System.nanoTime() - startNs;
      recordReplayMetrics(type, reason, elapsedNs);
      if (kind != null) {
        recordSchemaKindMetrics(kind, reason, elapsedNs);
        kindStats
            .computeIfAbsent(kind, k -> new KindStats())
            .record(elapsedNs, reason == Reason.TIMEOUT);
      }
      if (reason != null) {
        if (reason == Reason.TIMEOUT) {
          replayStats.timedOut.incrementAndGet();
        }
        replayStats.rejectedPositions.add(position);
        queryFilter.recordReplayRejection(reason);
      }
    } finally {
      // also cleared on an Error, so the query is not counted as running when warming finishes
      replayStats.inFlightPositions.remove(position);
    }
  }

  /** Record a replayed query, type is "full", "basic" or "schema". */
  private void recordReplayMetrics(String type, Reason reason, long elapsedNs) {
    String outcome = reason == null ? "success" : (reason == Reason.TIMEOUT ? "timeout" : "error");
    WarmingMetrics.warmingQueryCount.labelValues(index, type, outcome).inc();
    WarmingMetrics.warmingQueryLatency
        .labelValues(index, type)
        .observe(elapsedNs / (double) TimeUnit.SECONDS.toNanos(1));
  }

  private void recordSchemaKindMetrics(String kind, Reason reason, long elapsedNs) {
    String outcome = reason == null ? "success" : (reason == Reason.TIMEOUT ? "timeout" : "error");
    WarmingMetrics.warmingSchemaQueryCount.labelValues(index, kind, outcome).inc();
    WarmingMetrics.warmingSchemaQueryLatency
        .labelValues(index, kind)
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
