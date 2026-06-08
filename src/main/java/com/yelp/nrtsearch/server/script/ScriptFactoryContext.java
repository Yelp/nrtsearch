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
package com.yelp.nrtsearch.server.script;

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import java.util.Map;
import java.util.Objects;

/**
 * Bundles all request-level resources that a {@link ScoreScript.Factory} may need to produce a
 * {@link org.apache.lucene.search.DoubleValuesSource}. Using a single value object instead of
 * individual parameters makes the {@link ScoreScript.Factory#newFactory} signature stable — adding
 * resources in the future does not require changing the interface.
 */
public final class ScriptFactoryContext {

  private final Map<String, Object> params;
  private final DocLookup docLookup;
  private final SharedDocContext sharedDocContext;

  private ScriptFactoryContext(Builder builder) {
    this.params = builder.params;
    this.docLookup = builder.docLookup;
    this.sharedDocContext = builder.sharedDocContext;
  }

  /** Script parameters from the {@link com.yelp.nrtsearch.server.grpc.Script} request. */
  public Map<String, Object> getParams() {
    return params;
  }

  /** Index-level doc-values lookup. */
  public DocLookup getDocLookup() {
    return docLookup;
  }

  /**
   * Per-document context shared across pipeline stages. Retriever scores are stored here under the
   * key {@code retriever_<name>} as {@code Double} values, accessible via {@link
   * ScoreScript#getSharedDocContext()}.
   */
  public SharedDocContext getSharedDocContext() {
    return sharedDocContext;
  }

  @Override
  public int hashCode() {
    return Objects.hash(params, docLookup);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ScriptFactoryContext other)) {
      return false;
    }
    return Objects.equals(params, other.params) && Objects.equals(docLookup, other.docLookup);
  }

  @Override
  public String toString() {
    return "ScriptFactoryContext{params=" + params + ", docLookup=" + docLookup + "}";
  }

  public static Builder builder(Map<String, Object> params, DocLookup docLookup) {
    return new Builder(params, docLookup);
  }

  public static final class Builder {
    private final Map<String, Object> params;
    private final DocLookup docLookup;
    private SharedDocContext sharedDocContext;

    private Builder(Map<String, Object> params, DocLookup docLookup) {
      this.params = params;
      this.docLookup = docLookup;
    }

    public Builder sharedDocContext(SharedDocContext sharedDocContext) {
      this.sharedDocContext = sharedDocContext;
      return this;
    }

    public ScriptFactoryContext build() {
      return new ScriptFactoryContext(this);
    }
  }
}
