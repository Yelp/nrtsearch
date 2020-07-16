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
import com.yelp.nrtsearch.server.grpc.Point;
import com.yelp.nrtsearch.server.grpc.Selector;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;

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

        if (fd.valueSource != null) {
          sf = fd.valueSource.getSortField(sub.getReverse());
        } else if (fd.valueType == FieldDef.FieldValueType.LAT_LON) {
          if (fd.fieldType.docValuesType() == DocValuesType.NONE) {
            throw new SearchHandler.SearchHandlerException(
                String.format("field: %s was not registered with sort=true", fieldName));
          }
          Point sub2 = sub.getOrigin();
          sf =
              LatLonDocValuesField.newDistanceSort(
                  fieldName, sub2.getLatitude(), sub2.getLongitude());
        } else {
          if (fd.fieldType == null || fd.fieldType.docValuesType() == DocValuesType.NONE) {
            throw new SearchHandler.SearchHandlerException(
                String.format("field: %s was not registered with sort=true", fieldName));
          }

          if (fd.multiValued) {
            Selector selectorString = sub.getSelector();
            if (fd.valueType == FieldDef.FieldValueType.ATOM) {
              SortedSetSelector.Type selector;
              if (selectorString.equals(Selector.MIN)) {
                selector = SortedSetSelector.Type.MIN;
              } else if (selectorString.equals(Selector.MAX)) {
                selector = SortedSetSelector.Type.MAX;
              } else if (selectorString.equals(Selector.MIDDLE_MIN)) {
                selector = SortedSetSelector.Type.MIDDLE_MIN;
              } else if (selectorString.equals(Selector.MIDDLE_MAX)) {
                selector = SortedSetSelector.Type.MIDDLE_MAX;
              } else {
                assert false;
                // dead code but javac disagrees
                selector = null;
              }
              sf = new SortedSetSortField(fieldName, sub.getReverse(), selector);
            } else if (fd.valueType == FieldDef.FieldValueType.INT) {
              sf =
                  new SortedNumericSortField(
                      fieldName,
                      SortField.Type.INT,
                      sub.getReverse(),
                      parseNumericSelector(selectorString));
            } else if (fd.valueType == FieldDef.FieldValueType.LONG) {
              sf =
                  new SortedNumericSortField(
                      fieldName,
                      SortField.Type.LONG,
                      sub.getReverse(),
                      parseNumericSelector(selectorString));
            } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
              sf =
                  new SortedNumericSortField(
                      fieldName,
                      SortField.Type.FLOAT,
                      sub.getReverse(),
                      parseNumericSelector(selectorString));
            } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
              sf =
                  new SortedNumericSortField(
                      fieldName,
                      SortField.Type.DOUBLE,
                      sub.getReverse(),
                      parseNumericSelector(selectorString));
            } else {
              throw new SearchHandler.SearchHandlerException(
                  String.format(
                      "cannot sort by multiValued field: %s tyep is %s", fieldName, fd.valueType));
            }
          } else {
            SortField.Type sortType;
            if (fd.valueType == FieldDef.FieldValueType.ATOM) {
              sortType = SortField.Type.STRING;
            } else if (fd.valueType == FieldDef.FieldValueType.LONG
                || fd.valueType == FieldDef.FieldValueType.DATE_TIME) {
              sortType = SortField.Type.LONG;
            } else if (fd.valueType == FieldDef.FieldValueType.INT) {
              sortType = SortField.Type.INT;
            } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
              sortType = SortField.Type.DOUBLE;
            } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
              sortType = SortField.Type.FLOAT;
            } else {
              throw new SearchHandler.SearchHandlerException(
                  String.format("cannot sort by field: %s tyep is %s", fieldName, fd.valueType));
            }

            sf = new SortField(fieldName, sortType, sub.getReverse());
          }
        }

        boolean hasMissingLast = sub.getMissingLat();

        // TODO: SortType to have field missingLast?
        boolean missingLast = false;

        if (fd.valueType == FieldDef.FieldValueType.ATOM) {
          if (missingLast) {
            sf.setMissingValue(SortField.STRING_LAST);
          } else {
            sf.setMissingValue(SortField.STRING_FIRST);
          }
        } else if (fd.valueType == FieldDef.FieldValueType.INT) {
          sf.setMissingValue(missingLast ? Integer.MAX_VALUE : Integer.MIN_VALUE);
        } else if (fd.valueType == FieldDef.FieldValueType.LONG) {
          sf.setMissingValue(missingLast ? Long.MAX_VALUE : Long.MIN_VALUE);
        } else if (fd.valueType == FieldDef.FieldValueType.FLOAT) {
          sf.setMissingValue(missingLast ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY);
        } else if (fd.valueType == FieldDef.FieldValueType.DOUBLE) {
          sf.setMissingValue(missingLast ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
        } else if (hasMissingLast) {
          throw new SearchHandler.SearchHandlerException(
              String.format(
                  "field: %s can only specify missingLast for string and numeric field types: got SortField type: %s ",
                  fieldName, sf.getType()));
        }
      }
      sortFields.add(sf);
    }

    return new Sort(sortFields.toArray(new SortField[0]));
  }

  private static SortedNumericSelector.Type parseNumericSelector(Selector selectorString)
      throws SearchHandler.SearchHandlerException {
    if (selectorString.equals(Selector.MIN)) {
      return SortedNumericSelector.Type.MIN;
    } else if (selectorString.equals(Selector.MAX)) {
      return SortedNumericSelector.Type.MAX;
    } else {
      throw new SearchHandler.SearchHandlerException(
          "selector, must be min or max for multi-valued numeric sort fields");
    }
  }
}
