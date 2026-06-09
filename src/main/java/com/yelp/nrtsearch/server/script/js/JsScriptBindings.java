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
package com.yelp.nrtsearch.server.script.js;

import com.yelp.nrtsearch.server.doc.SharedDocContext;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

/**
 * Implements {@link Bindings} for a javascript expression. Binds to index fields and {@link
 * com.yelp.nrtsearch.server.grpc.Script} parameters.
 *
 * <p>When retrieving a binding, the parameter value is used if present. Otherwise, the field
 * binding is returned. Bound parameters will be converted to a double, with true being 1.0d and
 * false being 0.0d.
 *
 * <p>Bound parameters are also cached, since the value is constant for each query.
 *
 * <p>Shared doc context values are accessible via the {@code _shared_<key>} variable convention
 * (the {@code _shared_} prefix is stripped, and the remaining string is the key in the {@link
 * SharedDocContext} map for each document). Values must be stored as {@link Number} instances.
 * Retriever scores use the key format {@code retriever_<name>}, so the JS variable is {@code
 * _shared_retriever_<name>}. {@code advanceExact} returns {@code false} when no entry is present
 * for that document.
 */
public final class JsScriptBindings extends Bindings {

  static final String SHARED_CONTEXT_PREFIX = "_shared_";

  private final Bindings fieldDefBindings;
  private final Map<String, Object> scriptParams;
  private final Map<String, DoubleValuesSource> paramBindingsCache;
  private final SharedDocContext sharedDocContext;

  /**
   * Constructor without shared doc context.
   *
   * @param fieldDefBindings bindings for index fields
   * @param scriptParams params from a {@link com.yelp.nrtsearch.server.grpc.Script} definition
   * @throws NullPointerException if fieldDefBindings or scriptParams is null
   */
  public JsScriptBindings(Bindings fieldDefBindings, Map<String, Object> scriptParams) {
    this(fieldDefBindings, scriptParams, null);
  }

  /**
   * Constructor with shared doc context.
   *
   * @param fieldDefBindings bindings for index fields, may contain fields still being registered in
   *     a {@link com.yelp.nrtsearch.server.grpc.FieldDefRequest}
   * @param scriptParams params from a {@link com.yelp.nrtsearch.server.grpc.Script} definition,
   *     converted to java types
   * @param sharedDocContext per-document context shared across pipeline stages, or {@code null} if
   *     not available
   * @throws NullPointerException if fieldDefBindings or scriptParams is null
   */
  public JsScriptBindings(
      Bindings fieldDefBindings,
      Map<String, Object> scriptParams,
      SharedDocContext sharedDocContext) {
    Objects.requireNonNull(fieldDefBindings);
    Objects.requireNonNull(scriptParams);
    this.fieldDefBindings = fieldDefBindings;
    this.scriptParams = scriptParams;
    this.paramBindingsCache = scriptParams.isEmpty() ? Collections.emptyMap() : new HashMap<>();
    this.sharedDocContext = sharedDocContext;
  }

  /**
   * Get value binding for an expression variable. Resolution order:
   *
   * <ol>
   *   <li>Variables prefixed with {@value SHARED_CONTEXT_PREFIX} are resolved against {@link
   *       SharedDocContext} using the suffix as the key. Retriever scores use the key format {@code
   *       retriever_<name>}, so the JS variable is {@code _shared_retriever_<name>}.
   *   <li>Script parameter value (if present).
   *   <li>Document field value via {@code fieldDefBindings}.
   * </ol>
   *
   * @param name expression variable name
   * @return {@link DoubleValuesSource} factory to provide per segment variable evaluators.
   * @throws IllegalArgumentException if the named script parameter exists but is not of an expected
   *     type
   */
  @Override
  public DoubleValuesSource getDoubleValuesSource(String name) {
    if (name.startsWith(SHARED_CONTEXT_PREFIX)) {
      String key = name.substring(SHARED_CONTEXT_PREFIX.length());
      return new SharedContextDoubleValuesSource(sharedDocContext, key);
    }

    DoubleValuesSource cachedSource = paramBindingsCache.get(name);
    if (cachedSource != null) {
      return cachedSource;
    }

    Object paramObject = scriptParams.get(name);
    if (paramObject != null) {
      double valueAsDouble;
      if (paramObject instanceof Number) {
        valueAsDouble = ((Number) paramObject).doubleValue();
      } else if (paramObject instanceof Boolean value) {
        valueAsDouble = value == Boolean.TRUE ? 1.0 : 0.0;
      } else {
        throw new IllegalArgumentException(
            "Unable to bind script parameter name: " + name + " type: " + paramObject.getClass());
      }
      DoubleValuesSource paramDoubleValuesSource = new ParamDoubleValuesSource(valueAsDouble);
      paramBindingsCache.put(name, paramDoubleValuesSource);
      return paramDoubleValuesSource;
    }
    return fieldDefBindings.getDoubleValuesSource(name);
  }

  /**
   * Provides segment level {@link DoubleValues}. The value is constant and does not depend on any
   * segment data.
   */
  static class ParamDoubleValuesSource extends DoubleValuesSource {

    private final ParamDoubleValues paramDoubleValues;

    ParamDoubleValuesSource(double paramValue) {
      this.paramDoubleValues = new ParamDoubleValues(paramValue);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) {
      return paramDoubleValues;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
      return this;
    }

    @Override
    public int hashCode() {
      return Double.hashCode(paramDoubleValues.paramValue);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ParamDoubleValuesSource paramDoubleValuesSource) {
        return Double.compare(
                paramDoubleValuesSource.paramDoubleValues.paramValue, paramDoubleValues.paramValue)
            == 0;
      }
      return false;
    }

    @Override
    public String toString() {
      return "ParamDoubleValuesSource(" + paramDoubleValues.paramValue + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      // I'm not sure if this is lucene cacheable, but we do our own caching in the bindings anyways
      return false;
    }
  }

  /**
   * Provides the parameter value during expression evaluation. This value is constant for all
   * documents.
   */
  static class ParamDoubleValues extends DoubleValues {

    private final double paramValue;

    ParamDoubleValues(double paramValue) {
      this.paramValue = paramValue;
    }

    @Override
    public double doubleValue() {
      return paramValue;
    }

    @Override
    public boolean advanceExact(int doc) {
      return true;
    }
  }

  /**
   * {@link DoubleValuesSource} that resolves per-document values from {@link SharedDocContext} by
   * combining the segment leaf doc base with the segment-local doc id to form the global doc id.
   */
  static class SharedContextDoubleValuesSource extends DoubleValuesSource {

    private final SharedDocContext sharedDocContext;
    private final String key;

    SharedContextDoubleValuesSource(SharedDocContext sharedDocContext, String key) {
      this.sharedDocContext = sharedDocContext;
      this.key = key;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new SharedContextDoubleValues(sharedDocContext, key, ctx.docBase);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) {
      return this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SharedContextDoubleValuesSource other) {
        return Objects.equals(key, other.key) && sharedDocContext == other.sharedDocContext;
      }
      return false;
    }

    @Override
    public String toString() {
      return "SharedContextDoubleValuesSource(" + key + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  /** Resolves the shared context value for the current segment document as a double. */
  static class SharedContextDoubleValues extends DoubleValues {

    private final SharedDocContext sharedDocContext;
    private final String key;
    private final int leafDocBase;
    private double currentValue;

    SharedContextDoubleValues(SharedDocContext sharedDocContext, String key, int leafDocBase) {
      this.sharedDocContext = sharedDocContext;
      this.key = key;
      this.leafDocBase = leafDocBase;
    }

    @Override
    public double doubleValue() {
      return currentValue;
    }

    @Override
    public boolean advanceExact(int doc) {
      if (sharedDocContext == null) {
        return false;
      }
      Object value = sharedDocContext.getContext(leafDocBase + doc).get(key);
      if (value instanceof Number n) {
        currentValue = n.doubleValue();
        return true;
      }
      return false;
    }
  }
}
