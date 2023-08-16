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
package com.yelp.nrtsearch.server.luceneserver.search.sort;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class to handle creation of a {@link Sort} used to sort documents by field values for queries.
 */
public class SortParser {
  public static final BiFunction<SortField, Object, CompositeFieldValue>
      DEFAULT_SORT_VALUE_EXTRACTOR = (sortField, value) -> getValueForSortField(sortField, value);
  private static final String DOCID = "docid";
  private static final String SCORE = "score";

  private SortParser() {}

  /**
   * Decodes a list of request {@link SortType} into the corresponding {@link Sort}.
   *
   * @param fields list of {@link SortType} from grpc request
   * @param queryFields collection of all possible fields which may be used to sort
   */
  public static Sort parseSort(List<SortType> fields, Map<String, FieldDef> queryFields)
      throws SearchHandler.SearchHandlerException {
    List<SortField> sortFields = new ArrayList<>();
    for (SortType sub : fields) {
      String fieldName = sub.getFieldName();
      SortField sf;
      if (fieldName.equals(DOCID)) {
        if (!sub.getReverse()) {
          sf = SortField.FIELD_DOC;
        } else {
          sf = new SortField(null, SortField.Type.DOC, true);
        }
      } else if (fieldName.equals(SCORE)) {
        if (!sub.getReverse()) {
          sf = SortField.FIELD_SCORE;
        } else {
          sf = new SortField(null, SortField.Type.SCORE, true);
        }
      } else {
        FieldDef fd = queryFields.get(fieldName);
        if (fd == null) {
          throw new SearchHandler.SearchHandlerException(
              String.format(
                  "field: %s was not registered and was not specified as a virtualField",
                  fieldName));
        }

        if (!(fd instanceof Sortable)) {
          throw new IllegalArgumentException(
              String.format("field: %s does not support sorting", fieldName));
        }

        sf = ((Sortable) fd).getSortField(sub);
      }
      sortFields.add(sf);
    }

    return new Sort(sortFields.toArray(new SortField[0]));
  }

  /**
   * Validate the sort result of a {@link FieldDoc}, and parse the values. The method will try to
   * use the FieldDef specific parser to parse the value first if applicable; Otherwise, it will
   * check the value type and cast the value into the corresponding value object.
   *
   * @param fd Doc that contains the sorting values
   * @param sortContext sortContext for the sort configurations
   * @return sorted values for all sorted fields in map
   */
  public static Map<String, CompositeFieldValue> getAllSortedValues(
      FieldDoc fd, SortContext sortContext) {
    Sort sort = sortContext.getSort();
    if (fd.fields.length != sort.getSort().length) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and ScoreDoc: "
              + sort.getSort().length
              + " != "
              + fd.fields.length);
    }
    if (fd.fields.length != sortContext.getSortNames().size()) {
      throw new IllegalArgumentException(
          "Size mismatch between Sort and Sort names: "
              + fd.fields.length
              + " != "
              + sortContext.getSortNames().size());
    }

    Map<String, CompositeFieldValue> values = new HashMap<>(fd.fields.length);
    for (int i = 0; i < fd.fields.length; ++i) {
      SortField sortField = sort.getSort()[i];
      String fieldName = sortContext.getSortNames().get(i);
      values.put(
          fieldName, sortContext.getSortValueExtractors().get(i).apply(sortField, fd.fields[i]));
    }

    return values;
  }

  /**
   * Get the {@link SearchResponse.Hit.CompositeFieldValue} containing the sort value for the given
   * {@link SortField}.
   *
   * @param sortField sort field description
   * @param sortValue FieldDoc value for this sort field
   * @return hit message field value containing sort field value
   */
  private static SearchResponse.Hit.CompositeFieldValue getValueForSortField(
      SortField sortField, Object sortValue) {
    var fieldValue = SearchResponse.Hit.FieldValue.newBuilder();
    switch (sortField.getType()) {
      case DOC:
      case INT:
        fieldValue.setIntValue((Integer) sortValue);
        break;
      case SCORE:
      case FLOAT:
        fieldValue.setFloatValue((Float) sortValue);
        break;
      case LONG:
        fieldValue.setLongValue((Long) sortValue);
        break;
      case DOUBLE:
        fieldValue.setDoubleValue((Double) sortValue);
        break;
      case STRING:
      case STRING_VAL:
        fieldValue.setTextValue((String) sortValue);
        break;
      case CUSTOM:
        // could be anything, try to determine from value class
        fillFromValueClass(fieldValue, sortValue);
        break;
      default:
        throw new IllegalArgumentException(
            "Unable to get value for sort type: " + sortField.getType());
    }
    return SearchResponse.Hit.CompositeFieldValue.newBuilder().addFieldValue(fieldValue).build();
  }

  private static void fillFromValueClass(
      SearchResponse.Hit.FieldValue.Builder fieldValue, Object sortValue) {
    if (sortValue instanceof Double) {
      fieldValue.setDoubleValue((Double) sortValue);
    } else if (sortValue instanceof Float) {
      fieldValue.setFloatValue((Float) sortValue);
    } else if (sortValue instanceof Integer) {
      fieldValue.setIntValue((Integer) sortValue);
    } else if (sortValue instanceof Long) {
      fieldValue.setLongValue((Long) sortValue);
    } else if (sortValue instanceof Number) {
      fieldValue.setDoubleValue(((Number) sortValue).doubleValue());
    } else {
      throw new IllegalArgumentException("Unable to fill sort value: " + sortValue);
    }
  }
}
