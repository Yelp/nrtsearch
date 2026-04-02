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
package com.yelp.nrtsearch.server.search;

import com.yelp.nrtsearch.server.rescore.RescoreTask;
import com.yelp.nrtsearch.server.search.collectors.DocCollector;
import org.apache.lucene.search.Query;

public class RetrieverContext {
  private static final float DEFAULT_BOOST = 1.0f;

  /** Type of retriever, used to determine execution behavior in SearchHandler. */
  public enum RetrieverType {
    TEXT,
    KNN
  }

  private final String name;
  private final RetrieverType retrieverType;
  private final float boost;
  private final Query query;
  private final DocCollector docCollector;
  private final RescoreTask rescoreTask;

  private RetrieverContext(Builder builder) {
    this.name = builder.name;
    this.retrieverType = builder.retrieverType;
    this.boost = builder.boost > 0 ? builder.boost : DEFAULT_BOOST;
    this.query = builder.query;
    this.docCollector = builder.docCollector;
    this.rescoreTask = builder.rescoreTask;
  }

  public static Builder newBuilder(String name) {
    return new Builder(name);
  }

  public static class Builder {

    private final String name;
    private RetrieverType retrieverType;
    private float boost = DEFAULT_BOOST;
    private Query query;
    private DocCollector docCollector;
    private RescoreTask rescoreTask;

    private Builder(String name) {
      this.name = name;
    }

    public Builder retrieverType(RetrieverType retrieverType) {
      this.retrieverType = retrieverType;
      return this;
    }

    public Builder boost(float boost) {
      this.boost = boost;
      return this;
    }

    public Builder query(Query query) {
      this.query = query;
      return this;
    }

    public Builder docCollector(DocCollector docCollector) {
      this.docCollector = docCollector;
      return this;
    }

    public Builder rescoreTask(RescoreTask rescoreTask) {
      this.rescoreTask = rescoreTask;
      return this;
    }

    public RetrieverContext build() {
      return new RetrieverContext(this);
    }
  }

  public String getName() {
    return name;
  }

  public RetrieverType getRetrieverType() {
    return retrieverType;
  }

  public float getBoost() {
    return boost;
  }

  public Query getQuery() {
    return query;
  }

  public DocCollector getDocCollector() {
    return docCollector;
  }

  public RescoreTask getRescoreTask() {
    return rescoreTask;
  }
}
