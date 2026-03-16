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
package com.yelp.nrtsearch.server.search.retriever;

import com.yelp.nrtsearch.server.grpc.QuerySortField;
import org.apache.lucene.search.Query;

/**
 * Context for a single retriever within a {@link MultiRetrieverContext}. Holds the fully resolved
 * Lucene query (text or kNN) and retriever-level parameters extracted from the proto.
 */
public class RetrieverContext {
  private final String name;
  private final int startHit;
  private final int topHits;
  private final float boost;
  private final QuerySortField querySort;
  private final Query query;

  private RetrieverContext(Builder builder) {
    this.name = builder.name;
    this.startHit = builder.startHit;
    this.topHits = builder.topHits;
    this.boost = builder.boost;
    this.querySort = builder.querySort;
    this.query = builder.query;
  }

  /** Get the retriever name, used in diagnostics, per-retriever scores, and blender weight keys. */
  public String getName() {
    return name;
  }

  /** Get the offset of the first hit to return; default 0. */
  public int getStartHit() {
    return startHit;
  }

  /** Get the maximum number of documents to retrieve from this source. */
  public int getTopHits() {
    return topHits;
  }

  /** Get the boost applied to all scores before blending; default 1.0. */
  public float getBoost() {
    return boost;
  }

  /** Get the sort configuration, or {@code null} for relevance-based ranking. */
  public QuerySortField getQuerySort() {
    return querySort;
  }

  /** Get the fully resolved Lucene query for this retriever. */
  public Query getQuery() {
    return query;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private int startHit;
    private int topHits;
    private float boost = 1.0f;
    private QuerySortField querySort;
    private Query query;

    private Builder() {}

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setStartHit(int startHit) {
      this.startHit = startHit;
      return this;
    }

    public Builder setTopHits(int topHits) {
      this.topHits = topHits;
      return this;
    }

    public Builder setBoost(float boost) {
      this.boost = boost;
      return this;
    }

    public Builder setQuerySort(QuerySortField querySort) {
      this.querySort = querySort;
      return this;
    }

    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    public RetrieverContext build() {
      if (name == null || name.isEmpty()) {
        throw new IllegalStateException("name must be set");
      }
      if (query == null) {
        throw new IllegalStateException("query must be set");
      }
      return new RetrieverContext(this);
    }
  }
}
