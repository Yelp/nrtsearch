/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.sort;

import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Context class to convey all the sorted information, and provides helpers to retrieve the sorted
 * values.
 */
public class SortContext {
  private final Sort sort;
  private final List<String> sortNames;
  private final List<BiFunction<SortField, Object, CompositeFieldValue>> sortValueExtractors;

  public Sort getSort() {
    return sort;
  }

  public List<String> getSortNames() {
    return sortNames;
  }

  public List<BiFunction<SortField, Object, CompositeFieldValue>> getSortValueExtractors() {
    return sortValueExtractors;
  }

  public SortContext(QuerySortField querySortField, Map<String, FieldDef> queryFields) {
    try {
      this.sort =
          SortParser.parseSort(querySortField.getFields().getSortedFieldsList(), queryFields);
    } catch (SearchHandler.SearchHandlerException e) {
      throw new IllegalArgumentException(e);
    }
    this.sortNames = new ArrayList<>(querySortField.getFields().getSortedFieldsCount());
    this.sortValueExtractors = new ArrayList<>(querySortField.getFields().getSortedFieldsCount());
    for (SortType sortType : querySortField.getFields().getSortedFieldsList()) {
      // all validations are done in parserSort
      String fieldName = sortType.getFieldName();
      sortNames.add(fieldName);
      if (queryFields.containsKey(fieldName)) {
        sortValueExtractors.add(
            ((Sortable) queryFields.get(fieldName)).sortValueExtractor(sortType));
      } else {
        // for score and id, use the default extractor
        sortValueExtractors.add(SortParser.DEFAULT_SORT_VALUE_EXTRACTOR);
      }
    }
  }
}
