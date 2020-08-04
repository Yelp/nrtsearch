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
package com.yelp.nrtsearch.server.luceneserver.search;

import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class to handle creation of a {@link Sort} used to sort documents by field values for queries.
 */
public class SortParser {

  private static final String DOCID = "docid";
  private static final String SCORE = "score";

  private SortParser() {}

  /**
   * Decodes a list of request {@link SortType} into the corresponding {@link Sort}.
   *
   * @param fields list of {@link SortType} from grpc request
   * @param sortFieldNames mutable list which will have all sort field names added in sort order,
   *     may be null
   * @param queryFields collection of all possible fields which may be used to sort
   */
  public static Sort parseSort(
      List<SortType> fields, List<String> sortFieldNames, Map<String, FieldDef> queryFields)
      throws SearchHandler.SearchHandlerException {
    List<SortField> sortFields = new ArrayList<>();
    for (SortType sub : fields) {
      String fieldName = sub.getFieldName();
      SortField sf;
      if (sortFieldNames != null) {
        sortFieldNames.add(fieldName);
      }
      if (fieldName.equals(DOCID)) {
        sf = SortField.FIELD_DOC;
      } else if (fieldName.equals(SCORE)) {
        sf = SortField.FIELD_SCORE;
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
   * Get the name of a {@link SortField}. This will be either an index field name or a special sort
   * field name, such as 'docid' or 'score'.
   *
   * @param sortField sort field description
   * @return sort field name
   */
  public static String getNameForField(SortField sortField) {
    if (sortField == SortField.FIELD_DOC) {
      return DOCID;
    } else if (sortField == SortField.FIELD_SCORE) {
      return SCORE;
    } else {
      return sortField.getField();
    }
  }

  /**
   * Get the {@link com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue}
   * containing the sort value for the given {@link SortField}.
   *
   * @param sortField sort field description
   * @param fieldDoc lucene document hit
   * @param sortValue FieldDoc value for this sort field
   * @return hit message field value containing sort field value
   */
  public static SearchResponse.Hit.CompositeFieldValue getValueForSortField(
      SortField sortField, FieldDoc fieldDoc, Object sortValue) {
    var fieldValue = SearchResponse.Hit.FieldValue.newBuilder();
    switch (sortField.getType()) {
      case DOC:
        fieldValue.setIntValue(fieldDoc.doc);
        break;
      case SCORE:
        fieldValue.setFloatValue(fieldDoc.score);
        break;
      case INT:
        fieldValue.setIntValue((Integer) sortValue);
        break;
      case LONG:
        fieldValue.setLongValue((Long) sortValue);
        break;
      case FLOAT:
        fieldValue.setFloatValue((Float) sortValue);
        break;
      case DOUBLE:
        fieldValue.setDoubleValue((Double) sortValue);
        break;
      case STRING:
      case STRING_VAL:
        fieldValue.setTextValue((String) sortValue);
        break;
      default:
        throw new IllegalArgumentException(
            "Unable to get value for sort type: " + sortField.getType());
    }
    return SearchResponse.Hit.CompositeFieldValue.newBuilder().addFieldValue(fieldValue).build();
  }
}
