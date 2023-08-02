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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.HasSortableValueParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * * Context class to convey all the sorted information, and provides helpers to retrieve the sorted
 * values.
 */
public class SortContext {
  private final List<SortType> sortTypes;
  private final Sort sort;
  private final List<String> sortNames;
  private final Map<String, HasSortableValueParser> sortableValueParserMap;

  public Sort getSort() {
    return sort;
  }

  public SortContext(QuerySortField querySortField, Map<String, FieldDef> queryFields) {
    this.sortTypes = querySortField.getFields().getSortedFieldsList();
    this.sortNames = new ArrayList<>(querySortField.getFields().getSortedFieldsCount());
    this.sortableValueParserMap = new HashMap<>(querySortField.getFields().getSortedFieldsCount());
    try {
      this.sort =
          SortParser.parseSort(
              querySortField.getFields().getSortedFieldsList(),
              this.sortNames,
              this.sortableValueParserMap,
              queryFields);
    } catch (SearchHandler.SearchHandlerException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * * validate the sort result of a {@link FieldDoc}, and parse the values. The method will try to
   * use the FieldDef specific parser to parse the value first if applicable; Otherwise, it will
   * check the value type and cast the value into the corresponding value object.
   *
   * @param fd Doc that contains the sorting values
   * @return sorted values for all sorted fields in map
   */
  public Map<String, CompositeFieldValue> getAllSortedValues(FieldDoc fd) {
    if (fd.fields.length != sort.getSort().length) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and ScoreDoc: "
              + sort.getSort().length
              + " != "
              + fd.fields.length);
    }
    if (fd.fields.length != sortNames.size()) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and Sort names: "
              + fd.fields.length
              + " != "
              + sortNames.size());
    }

    Map<String, CompositeFieldValue> values = new HashMap<>(fd.fields.length);
    for (int i = 0; i < fd.fields.length; ++i) {
      SortField sortField = sort.getSort()[i];
      String fieldName = sortNames.get(i);
      if (sortableValueParserMap != null && sortableValueParserMap.containsKey(fieldName)) {
        // use fieldDef specific parser to parse the value
        values.put(
            fieldName,
            sortableValueParserMap.get(fieldName).parseSortedValue(sortTypes.get(i), fd.fields[i]));
      } else {
        values.put(fieldName, SortParser.getValueForSortField(sortField, fd.fields[i]));
      }
    }

    return values;
  }
}
