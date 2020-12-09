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

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;

public class ObjectFieldDef extends IndexableFieldDef {

  private final Gson gson;

  protected ObjectFieldDef(String name, Field requestField) {
    super(name, requestField);
    gson = new Gson();
  }

  @Override
  public String getType() {
    return "OBJECT";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {}

  @Override
  @SuppressWarnings("unchecked")
  public void parseFieldWithChildren(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    List<Map<String, Object>> fieldValueMaps = new ArrayList<>();
    fieldValues.stream().map(e -> gson.fromJson(e, Map.class)).forEach(e -> fieldValueMaps.add(e));
    parseFieldWithChildrenObject(document, fieldValueMaps, facetHierarchyPaths);
  }

  public void parseFieldWithChildrenObject(
      Document document,
      List<Map<String, Object>> fieldValues,
      List<List<String>> facetHierarchyPaths) {
    for (Map.Entry<String, IndexableFieldDef> childField : this.getChildFields().entrySet()) {
      String[] keys = childField.getKey().split("\\.");
      String key = keys[keys.length - 1];
      if (childField.getValue().getType().equals("object")) {
        List<Map<String, Object>> childrenValues = new ArrayList<>();
        for (Map<String, Object> fieldValue : fieldValues) {
          Object childValue = fieldValue.get(key);
          if (childValue != null) {
            if (childValue instanceof Map) {
              childrenValues.add((Map<String, Object>) childValue);
            } else if (childValue instanceof List) {
              childrenValues.addAll((List<Map<String, Object>>) childValue);
            } else {
              throw new IllegalArgumentException("Invalid data");
            }
          }
        }
        ((ObjectFieldDef) childField.getValue())
            .parseFieldWithChildrenObject(document, childrenValues, facetHierarchyPaths);
      } else {
        List<String> childrenValues = new ArrayList<>();
        for (Map<String, Object> fieldValue : fieldValues) {
          Object childValue = fieldValue.get(key);
          if (childValue != null) {
            if (childValue instanceof List) {
              ((List<Object>) childValue)
                  .stream().forEach(e -> childrenValues.add(String.valueOf(e)));
            } else {
              childrenValues.add(String.valueOf(childValue));
            }
          }
        }
        childField.getValue().parseFieldWithChildren(document, childrenValues, facetHierarchyPaths);
      }
    }
  }
}
