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
package com.yelp.nrtsearch.server.luceneserver.script.js;

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
 */
public final class JsScriptBindings extends Bindings {

  private final Bindings fieldDefBindings;
  private final Map<String, Object> scriptParams;
  private final Map<String, DoubleValuesSource> paramBindingsCache;

  /**
   * Sole constructor.
   *
   * @param fieldDefBindings bindings for index fields, may contain fields still being registers in
   *     a {@link com.yelp.nrtsearch.server.grpc.FieldDefRequest}
   * @param scriptParams params from a {@link com.yelp.nrtsearch.server.grpc.Script} definition,
   *     converted to java types
   * @throws NullPointerException if fieldDefBindings or scriptParams is null
   */
  public JsScriptBindings(Bindings fieldDefBindings, Map<String, Object> scriptParams) {
    Objects.requireNonNull(fieldDefBindings);
    Objects.requireNonNull(scriptParams);
    this.fieldDefBindings = fieldDefBindings;
    this.scriptParams = scriptParams;
    this.paramBindingsCache = scriptParams.isEmpty() ? Collections.emptyMap() : new HashMap<>();
  }

  /**
   * Get value binding for an expression variable. Binds to script parameter value if present,
   * otherwise the document field value.
   *
   * @param name expression variable name
   * @return {@link DoubleValuesSource} factory to provide per segment variable evaluators.
   * @throws IllegalArgumentException if then named script parameter exists, but is not of an
   *     expected type
   */
  @Override
  public DoubleValuesSource getDoubleValuesSource(String name) {
    DoubleValuesSource cachedSource = paramBindingsCache.get(name);
    if (cachedSource != null) {
      return cachedSource;
    }

    Object paramObject = scriptParams.get(name);
    if (paramObject != null) {
      double valueAsDouble;
      if (paramObject instanceof Number) {
        valueAsDouble = ((Number) paramObject).doubleValue();
      } else if (paramObject instanceof Boolean) {
        Boolean value = (Boolean) paramObject;
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
      if (obj instanceof ParamDoubleValuesSource) {
        ParamDoubleValuesSource paramDoubleValuesSource = (ParamDoubleValuesSource) obj;
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
}
