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
package com.yelp.nrtsearch.server.luceneserver.field;

import com.yelp.nrtsearch.server.luceneserver.field.properties.Bindable;
import java.util.Map;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.search.DoubleValuesSource;

/** Implements {@link Bindings} on top of the registered fields. */
public final class FieldDefBindings extends Bindings {

  private final Map<String, FieldDef> fields;

  /** Sole constructor. */
  public FieldDefBindings(Map<String, FieldDef> fields) {
    this.fields = fields;
  }

  @Override
  public DoubleValuesSource getDoubleValuesSource(String name) {
    if (name.equals("_score")) {
      return DoubleValuesSource.SCORES;
    }
    FieldDef fd = fields.get(name);
    if (fd == null) {
      throw new IllegalArgumentException("Invalid reference '" + name + "'");
    }
    if (!(fd instanceof Bindable)) {
      throw new IllegalArgumentException("Field: " + name + " does not support expression binding");
    }
    return ((Bindable) fd).getExpressionBinding();
  }
}
