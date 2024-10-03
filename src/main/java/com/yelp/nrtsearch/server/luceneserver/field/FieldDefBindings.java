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
import java.util.function.Function;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.js.VariableContext;
import org.apache.lucene.expressions.js.VariableContext.Type;
import org.apache.lucene.search.DoubleValuesSource;

/** Implements {@link Bindings} on top of the registered fields. */
public final class FieldDefBindings extends Bindings {

  private final Function<String, FieldDef> fieldDefLookup;

  /** Sole constructor. */
  public FieldDefBindings(Function<String, FieldDef> fieldDefLookup) {
    this.fieldDefLookup = fieldDefLookup;
  }

  /**
   * Bind a field name into a lucene {@link org.apache.lucene.expressions.Expression} by providing a
   * {@link DoubleValuesSource}.
   *
   * <p>Valid values are:
   *
   * <p>_score : relevance score
   *
   * <p>field_name : the name of a field that implements {@link Bindable}, provides the field value
   * (or first value if the field is multi value)
   *
   * <p>doc['field_name'].property : the field_name must be a field implementing {@link Bindable},
   * and the property must be an implemented bindable property of the field such as value, length,
   * or empty
   *
   * @param name variable name
   * @return expression value source
   */
  @Override
  public DoubleValuesSource getDoubleValuesSource(String name) {
    if (name.equals("_score")) {
      return DoubleValuesSource.SCORES;
    }
    FieldDef fd = fieldDefLookup.apply(name);
    String property = Bindable.VALUE_PROPERTY;
    String fieldName = name;
    if (fd == null) {
      // name is not a field name, try to parse it as the extended form doc['field_name'].property
      VariableContext[] parsed = VariableContext.parse(name);
      if (parsed.length != 3) {
        throw new IllegalArgumentException("Invalid reference '" + name + "'");
      }
      if (!parsed[0].type.equals(Type.MEMBER)
          || !parsed[0].text.equals("doc")
          || !parsed[1].type.equals(Type.STR_INDEX)
          || parsed[2].type.equals(Type.INT_INDEX)) {
        throw new IllegalArgumentException(
            "Invalid field binding format: " + name + ", expected: doc['field_name'].property");
      }
      fieldName = parsed[1].text;
      fd = fieldDefLookup.apply(fieldName);
      if (fd == null) {
        throw new IllegalArgumentException("Unknown field to bind: " + fieldName);
      }
      property = parsed[2].text;
    }
    if (!(fd instanceof Bindable)) {
      throw new IllegalArgumentException(
          "Field: " + fieldName + " does not support expression binding");
    }
    return ((Bindable) fd).getExpressionBinding(property);
  }
}
