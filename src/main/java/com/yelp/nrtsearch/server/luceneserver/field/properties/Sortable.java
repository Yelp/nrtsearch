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
package com.yelp.nrtsearch.server.luceneserver.field.properties;

import com.yelp.nrtsearch.server.grpc.Selector;
import com.yelp.nrtsearch.server.grpc.SortType;
import java.util.function.Function;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;

/**
 * Trait interface for {@link com.yelp.nrtsearch.server.luceneserver.field.FieldDef} types that can
 * be used as a {@link SortField} during search or indexing.
 */
public interface Sortable {
  Function<Selector, SortedNumericSelector.Type> NUMERIC_TYPE_PARSER =
      selector -> {
        switch (selector) {
          case MIN:
            return SortedNumericSelector.Type.MIN;
          case MAX:
            return SortedNumericSelector.Type.MAX;
          default:
            throw new IllegalArgumentException(
                "selector must be min or max for multi-valued numeric sort fields");
        }
      };
  Function<Selector, SortedSetSelector.Type> SORTED_SET_TYPE_PARSER =
      selector -> {
        switch (selector) {
          case MIN:
            return SortedSetSelector.Type.MIN;
          case MAX:
            return SortedSetSelector.Type.MAX;
          case MIDDLE_MIN:
            return SortedSetSelector.Type.MIDDLE_MIN;
          case MIDDLE_MAX:
            return SortedSetSelector.Type.MIDDLE_MAX;
          default:
            throw new IllegalArgumentException("Unknown selector value: " + selector);
        }
      };

  /**
   * Build a lucene {@link SortField} for this field type with the given type configuration.
   *
   * @param type sort configuration
   * @return sort field for this type
   */
  SortField getSortField(SortType type);
}
