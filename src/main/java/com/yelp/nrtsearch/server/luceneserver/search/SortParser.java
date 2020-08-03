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

import com.google.common.collect.ImmutableSet;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class to handle creation of a {@link Sort} used to sort documents by field values for queries.
 */
public class SortParser {

  public static final Set<String> SPECIAL_FIELDS =
      ImmutableSet.<String>builder().add("docid").add("score").build();

  private SortParser() {}

  /**
   * Decodes a list of Request into the corresponding Sort.
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
      if (fieldName.equals("docid")) {
        sf = SortField.FIELD_DOC;
      } else if (fieldName.equals("score")) {
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
}
