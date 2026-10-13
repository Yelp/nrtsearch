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
package com.yelp.nrtsearch.server.warming;

import com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;

/**
 * Decides whether a query is suitable for the warming corpus. Pathological queries (match-all
 * scans, very large result sets, slow queries) are cheap enough under warm caches to go unnoticed
 * in live traffic, but replaying them against a cold index on startup can stall warming entirely.
 *
 * <p>The same filter is applied when a query is collected from live traffic and again when the
 * corpus is backed up, so queries that entered the corpus by another path (padding from a previous
 * corpus, or collection before a filter was configured) are also evicted.
 */
public class WarmingQueryFilter {

  /** Reason a query was rejected. */
  public enum Reason {
    MATCH_ALL,
    TOP_HITS,
    LATENCY,
    TIMEOUT,
    ERROR;

    String label() {
      return name().toLowerCase();
    }
  }

  /** Point at which the query was rejected, used as a metric label. */
  public enum Stage {
    COLLECTION,
    BACKUP,
    REPLAY;

    String label() {
      return name().toLowerCase();
    }
  }

  private final String index;
  private final boolean filterMatchAllQueries;
  private final int maxQueryTopHits;
  private final int maxQueryLatencyMs;

  /**
   * @param index index name, used for metrics
   * @param filterMatchAllQueries reject queries that match all documents
   * @param maxQueryTopHits reject queries with startHit + topHits above this value, 0 to disable
   * @param maxQueryLatencyMs reject collected queries that took longer than this, 0 to disable
   */
  public WarmingQueryFilter(
      String index, boolean filterMatchAllQueries, int maxQueryTopHits, int maxQueryLatencyMs) {
    this.index = index;
    this.filterMatchAllQueries = filterMatchAllQueries;
    this.maxQueryTopHits = maxQueryTopHits;
    this.maxQueryLatencyMs = maxQueryLatencyMs;
  }

  /** Filter that accepts every query. */
  public static WarmingQueryFilter acceptAll(String index) {
    return new WarmingQueryFilter(index, false, 0, 0);
  }

  /**
   * Check a query observed in live traffic.
   *
   * @param searchRequest query to check
   * @param latencyMs time taken to execute the query
   * @return reason the query should not be collected, or null if it may be collected
   */
  public Reason checkCollected(SearchRequest searchRequest, double latencyMs) {
    Reason reason = checkRequest(searchRequest);
    if (reason == null && maxQueryLatencyMs > 0 && latencyMs > maxQueryLatencyMs) {
      reason = Reason.LATENCY;
    }
    return record(Stage.COLLECTION, reason);
  }

  /**
   * Check a query about to be written to the warming corpus.
   *
   * @param searchRequest query to check
   * @return reason the query should not be backed up, or null if it may be backed up
   */
  public Reason checkBackup(SearchRequest searchRequest) {
    return record(Stage.BACKUP, checkRequest(searchRequest));
  }

  /** Count a query that was rejected while being replayed. */
  void recordReplayRejection(Reason reason) {
    record(Stage.REPLAY, reason);
  }

  private Reason checkRequest(SearchRequest searchRequest) {
    if (filterMatchAllQueries && isMatchAll(searchRequest)) {
      return Reason.MATCH_ALL;
    }
    if (maxQueryTopHits > 0
        && (long) searchRequest.getStartHit() + searchRequest.getTopHits() > maxQueryTopHits) {
      return Reason.TOP_HITS;
    }
    return null;
  }

  private Reason record(Stage stage, Reason reason) {
    if (reason != null) {
      WarmingMetrics.warmingQueryRejectedCount
          .labelValues(index, stage.label(), reason.label())
          .inc();
    }
    return reason;
  }

  /**
   * A request with no query, no query text, no knn and no multi-retriever is executed as a {@link
   * org.apache.lucene.search.MatchAllDocsQuery}, as is an explicit match all query node.
   */
  static boolean isMatchAll(SearchRequest searchRequest) {
    if (searchRequest.hasMultiRetriever()
        || searchRequest.getKnnCount() > 0
        || !searchRequest.getQueryText().isEmpty()) {
      return false;
    }
    QueryNodeCase queryCase = searchRequest.getQuery().getQueryNodeCase();
    return queryCase == QueryNodeCase.MATCHALLQUERY || queryCase == QueryNodeCase.QUERYNODE_NOT_SET;
  }
}
