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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery.ZeroTerms;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import java.util.Map;

/**
 * Decides whether a query is suitable for the warming corpus. Pathological queries (match-all
 * scans, very large result sets, deep pagination, large vector searches, slow queries) are cheap
 * enough under warm caches to go unnoticed in live traffic, but replaying them against a cold index
 * on startup can stall warming entirely.
 *
 * <p>The same checks are applied when a query is collected from live traffic, when the corpus is
 * backed up, and before a stored query is replayed, so queries that entered the corpus by another
 * path (padding from a previous corpus, or collection before a filter was configured) are also
 * excluded. For multi retriever requests, the checks are applied to each retriever.
 */
public class WarmingQueryFilter {

  /** Reason a query was rejected. */
  public enum Reason {
    MATCH_ALL,
    ALL_ZERO_TERMS,
    TOP_HITS,
    VECTOR_CANDIDATES,
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
  private final int maxVectorCandidates;

  /**
   * @param index index name, used for metrics
   * @param filterMatchAllQueries reject queries that match all documents, including match phrase
   *     queries that match all documents when analysis removes all terms
   * @param maxQueryTopHits reject queries with topHits or startHit above this value, 0 to disable
   * @param maxQueryLatencyMs reject collected queries that took longer than this, 0 to disable
   * @param maxVectorCandidates reject queries with a knn query that has numCandidates above this
   *     value, 0 to disable
   */
  public WarmingQueryFilter(
      String index,
      boolean filterMatchAllQueries,
      int maxQueryTopHits,
      int maxQueryLatencyMs,
      int maxVectorCandidates) {
    this.index = index;
    this.filterMatchAllQueries = filterMatchAllQueries;
    this.maxQueryTopHits = maxQueryTopHits;
    this.maxQueryLatencyMs = maxQueryLatencyMs;
    this.maxVectorCandidates = maxVectorCandidates;
  }

  /** Filter that accepts every query. */
  public static WarmingQueryFilter acceptAll(String index) {
    return new WarmingQueryFilter(index, false, 0, 0, 0);
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

  /**
   * Check a stored query before it is replayed, since the stored corpus may predate the filter.
   *
   * @param searchRequest query to check, as it will be executed
   * @return reason the query should not be replayed, or null if it may be replayed
   */
  public Reason checkReplay(SearchRequest searchRequest) {
    return record(Stage.REPLAY, checkRequest(searchRequest));
  }

  /** Count a query that was rejected while being replayed. */
  void recordReplayRejection(Reason reason) {
    record(Stage.REPLAY, reason);
  }

  private Reason checkRequest(SearchRequest searchRequest) {
    if (filterMatchAllQueries) {
      if (isMatchAll(searchRequest)) {
        return Reason.MATCH_ALL;
      }
      if (hasAllZeroTerms(searchRequest.getQuery())) {
        return Reason.ALL_ZERO_TERMS;
      }
    }
    // topHits already includes startHit, but check startHit too in case a client sets it alone
    if (maxQueryTopHits > 0
        && (searchRequest.getTopHits() > maxQueryTopHits
            || searchRequest.getStartHit() > maxQueryTopHits)) {
      return Reason.TOP_HITS;
    }
    if (exceedsVectorCandidates(searchRequest.getKnnList())) {
      return Reason.VECTOR_CANDIDATES;
    }
    for (Retriever retriever : searchRequest.getMultiRetriever().getRetrieversList()) {
      Reason reason = checkRetriever(retriever);
      if (reason != null) {
        return reason;
      }
    }
    return null;
  }

  private Reason checkRetriever(Retriever retriever) {
    switch (retriever.getRetrieverTypeCase()) {
      case TEXTRETRIEVER -> {
        TextRetriever textRetriever = retriever.getTextRetriever();
        if (filterMatchAllQueries) {
          if (isMatchAll(textRetriever.getQuery())) {
            return Reason.MATCH_ALL;
          }
          if (hasAllZeroTerms(textRetriever.getQuery())) {
            return Reason.ALL_ZERO_TERMS;
          }
        }
        if (maxQueryTopHits > 0 && textRetriever.getTopHits() > maxQueryTopHits) {
          return Reason.TOP_HITS;
        }
      }
      case KNNRETRIEVER -> {
        if (exceedsVectorCandidates(retriever.getKnnRetriever().getKnnQuery())) {
          return Reason.VECTOR_CANDIDATES;
        }
      }
      default -> {}
    }
    return null;
  }

  private boolean exceedsVectorCandidates(Iterable<KnnQuery> knnQueries) {
    for (KnnQuery knnQuery : knnQueries) {
      if (exceedsVectorCandidates(knnQuery)) {
        return true;
      }
    }
    return false;
  }

  private boolean exceedsVectorCandidates(KnnQuery knnQuery) {
    // numCandidates must be >= k, so it bounds the vectors visited per segment
    return maxVectorCandidates > 0 && knnQuery.getNumCandidates() > maxVectorCandidates;
  }

  private Reason record(Stage stage, Reason reason) {
    if (reason != null) {
      WarmingMetrics.warmingQueriesFilteredCount
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
    return isMatchAll(searchRequest.getQuery());
  }

  /** An unset query or an explicit match all query node matches all documents. */
  static boolean isMatchAll(Query query) {
    QueryNodeCase queryCase = query.getQueryNodeCase();
    return queryCase == QueryNodeCase.MATCHALLQUERY || queryCase == QueryNodeCase.QUERYNODE_NOT_SET;
  }

  /**
   * Whether the query tree contains a match phrase query with {@link ZeroTerms#ALL_ZERO_TERMS},
   * which matches all documents when analysis removes all terms (for example a query of only stop
   * words). The tree is walked generically, so this covers every query type that can nest queries.
   */
  static boolean hasAllZeroTerms(Message message) {
    if (message instanceof MatchPhraseQuery matchPhraseQuery) {
      return matchPhraseQuery.getZeroTermsQuery() == ZeroTerms.ALL_ZERO_TERMS;
    }
    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      FieldDescriptor field = entry.getKey();
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      if (field.isRepeated()) {
        for (Object value : (Iterable<?>) entry.getValue()) {
          if (value instanceof Message child && hasAllZeroTerms(child)) {
            return true;
          }
        }
      } else if (hasAllZeroTerms((Message) entry.getValue())) {
        return true;
      }
    }
    return false;
  }
}
