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

import static com.yelp.nrtsearch.server.analysis.AnalyzerCreator.isAnalyzerDefined;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.yelp.nrtsearch.server.analysis.AnalyzerCreator;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanClause.Occur;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery;
import com.yelp.nrtsearch.server.grpc.MatchPhraseQuery.ZeroTerms;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Query.QueryNodeCase;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.monitoring.WarmingMetrics;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.QueryBuilder;

/**
 * Decides whether a query is suitable for the warming corpus. Pathological queries (match-all
 * scans, very large result sets, deep pagination, large vector searches, slow queries) are cheap
 * enough under warm caches to go unnoticed in live traffic, but replaying them against a cold index
 * on startup can stall warming entirely.
 *
 * <p>The same checks are applied when a query is collected from live traffic and before a stored
 * query is replayed, so a stored corpus that predates the filter is also cleaned up. Downloaded
 * queries are only used to pad the next backup if they pass the replay check. For multi retriever
 * requests, the checks are applied to each retriever.
 */
public class WarmingQueryFilter {

  /** Reason a query was rejected. */
  public enum Reason {
    MATCH_ALL,
    ALL_ZERO_TERMS,
    // the simplified basic query matches all documents, the original query is still kept
    BASIC_MATCH_ALL,
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
   *     queries that match all documents because analysis removes all terms
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
   * @param docLookup lookup for the index fields, used to analyze match phrase queries, may be null
   * @return reason the query should not be collected, or null if it may be collected
   */
  public Reason checkCollected(SearchRequest searchRequest, double latencyMs, DocLookup docLookup) {
    Reason reason = checkRequest(searchRequest, docLookup);
    if (reason == null && maxQueryLatencyMs > 0 && latencyMs > maxQueryLatencyMs) {
      reason = Reason.LATENCY;
    }
    return record(Stage.COLLECTION, reason);
  }

  /**
   * Check a stored query before it is replayed, since the stored corpus may predate the filter.
   *
   * @param searchRequest query to check
   * @param docLookup lookup for the index fields, used to analyze match phrase queries, may be null
   * @return reason the query should not be replayed, or null if it may be replayed
   */
  public Reason checkReplay(SearchRequest searchRequest, DocLookup docLookup) {
    return record(Stage.REPLAY, checkRequest(searchRequest, docLookup));
  }

  /**
   * Check the simplified form of a stored query that passed {@link #checkReplay}. Simplifying can
   * turn a query into a match all scan, such as a top level function filter query. Rejections are
   * recorded as {@link Reason#BASIC_MATCH_ALL}, since the original query is still kept.
   *
   * @param basicRequest simplified query, as it will be executed
   * @param docLookup lookup for the index fields, used to analyze match phrase queries, may be null
   * @return reason the simplified query should not be replayed, or null if it may be replayed
   */
  public Reason checkBasicReplay(SearchRequest basicRequest, DocLookup docLookup) {
    Reason reason = checkRequest(basicRequest, docLookup) == null ? null : Reason.BASIC_MATCH_ALL;
    return record(Stage.REPLAY, reason);
  }

  /** Count a query that was rejected while being replayed. */
  void recordReplayRejection(Reason reason) {
    record(Stage.REPLAY, reason);
  }

  private Reason checkRequest(SearchRequest searchRequest, DocLookup docLookup) {
    if (filterMatchAllQueries) {
      if (isMatchAll(searchRequest)) {
        return Reason.MATCH_ALL;
      }
      if (hasAllZeroTermsScan(searchRequest.getQuery(), docLookup)) {
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
      Reason reason = checkRetriever(retriever, docLookup);
      if (reason != null) {
        return reason;
      }
    }
    return null;
  }

  private Reason checkRetriever(Retriever retriever, DocLookup docLookup) {
    switch (retriever.getRetrieverTypeCase()) {
      case TEXTRETRIEVER -> {
        TextRetriever textRetriever = retriever.getTextRetriever();
        if (filterMatchAllQueries) {
          if (isMatchAll(textRetriever.getQuery())) {
            return Reason.MATCH_ALL;
          }
          if (hasAllZeroTermsScan(textRetriever.getQuery(), docLookup)) {
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
   * Whether the query tree contains a match phrase query with {@link ZeroTerms#ALL_ZERO_TERMS} that
   * analysis reduces to no terms, which is executed as a {@link
   * org.apache.lucene.search.MatchAllDocsQuery}. Boolean queries with a restrictive MUST or FILTER
   * clause are not searched, since they are not a full scan even if another clause is. The rest of
   * the tree is walked generically, so this covers every query type that can nest queries.
   *
   * @param message query tree to check
   * @param docLookup lookup for the field search analyzer, used when the query does not set its own
   *     analyzer. If null, such queries are assumed to have terms.
   */
  static boolean hasAllZeroTermsScan(Message message, DocLookup docLookup) {
    if (message instanceof MatchPhraseQuery matchPhraseQuery) {
      return matchPhraseQuery.getZeroTermsQuery() == ZeroTerms.ALL_ZERO_TERMS
          && analyzesToNoTerms(matchPhraseQuery, docLookup);
    }
    if (message instanceof BooleanQuery booleanQuery
        && hasRestrictiveClause(booleanQuery, docLookup)) {
      return false;
    }
    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      FieldDescriptor field = entry.getKey();
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      if (field.isRepeated()) {
        for (Object value : (Iterable<?>) entry.getValue()) {
          if (value instanceof Message child && hasAllZeroTermsScan(child, docLookup)) {
            return true;
          }
        }
      } else if (hasAllZeroTermsScan((Message) entry.getValue(), docLookup)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasRestrictiveClause(BooleanQuery booleanQuery, DocLookup docLookup) {
    for (BooleanClause clause : booleanQuery.getClausesList()) {
      if ((clause.getOccur() == Occur.MUST || clause.getOccur() == Occur.FILTER)
          && !isMatchAll(clause.getQuery())
          && !hasAllZeroTermsScan(clause.getQuery(), docLookup)) {
        return true;
      }
    }
    return false;
  }

  /** Analyze the phrase the same way as {@code QueryNodeMapper}, true if no terms remain. */
  private static boolean analyzesToNoTerms(MatchPhraseQuery matchPhraseQuery, DocLookup docLookup) {
    try {
      Analyzer analyzer;
      if (isAnalyzerDefined(matchPhraseQuery.getAnalyzer())) {
        analyzer = AnalyzerCreator.getInstance().getAnalyzer(matchPhraseQuery.getAnalyzer());
      } else if (docLookup != null) {
        analyzer = IndexState.getFieldSearchAnalyzer(matchPhraseQuery.getField(), docLookup);
      } else {
        return false;
      }
      return new QueryBuilder(analyzer)
              .createPhraseQuery(
                  matchPhraseQuery.getField(),
                  matchPhraseQuery.getQuery(),
                  matchPhraseQuery.getSlop())
          == null;
    } catch (Exception e) {
      // the query cannot be built, so it is not a full scan
      return false;
    }
  }
}
