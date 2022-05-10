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
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.AddDocumentHandler;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

public class ObjectFieldDef extends IndexableFieldDef {

  private final Gson gson;
  private final boolean isNestedDoc;

  protected ObjectFieldDef(String name, Field requestField) {
    super(name, requestField);
    this.isNestedDoc = requestField.getNestedDoc();
    gson = new GsonBuilder().serializeNulls().create();
  }

  @Override
  public String getType() {
    return "OBJECT";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {}

  @Override
  public void parseFieldWithChildren(
      AddDocumentHandler.DocumentsContext documentsContext,
      List<String> fieldValues,
      List<List<String>> facetHierarchyPaths) {
    if (!isNestedDoc) {
      parseFieldWithChildren(documentsContext.getRootDocument(), fieldValues, facetHierarchyPaths);
    } else {
      List<Map<String, Object>> fieldValueMaps = new ArrayList<>();
      fieldValues.stream()
          .map(e -> gson.fromJson(e, Map.class))
          .forEach(e -> fieldValueMaps.add(e));

      List<Document> childDocuments =
          fieldValueMaps.stream()
              .map(e -> createChildDocument(e, facetHierarchyPaths))
              .collect(Collectors.toList());
      documentsContext.addChildDocuments(this.getName(), childDocuments);
    }
  }

  /**
   * create a new lucene document for each nested object
   *
   * @param fieldValue
   * @param facetHierarchyPaths
   * @return lucene document
   */
  private Document createChildDocument(
      Map<String, Object> fieldValue, List<List<String>> facetHierarchyPaths) {
    Document document = new Document();
    parseFieldWithChildrenObject(document, List.of(fieldValue), facetHierarchyPaths);
    ((IndexableFieldDef) (IndexState.getMetaField(IndexState.NESTED_PATH)))
        .parseDocumentField(document, List.of(this.getName()), List.of());
    return document;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void parseFieldWithChildren(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    List<Map<String, Object>> fieldValueMaps = new ArrayList<>();
    fieldValues.stream().map(e -> gson.fromJson(e, Map.class)).forEach(e -> fieldValueMaps.add(e));
    if (isStored()) {
      for (String fieldValue : fieldValues) {
        document.add(new StoredField(this.getName(), fieldValue));
      }
    }
    if (hasDocValues()) {
      document.add(
          new BinaryDocValuesField(getName(), new BytesRef(wrapJsonStringList(fieldValues))));
    }
    parseFieldWithChildrenObject(document, fieldValueMaps, facetHierarchyPaths);
  }

  public void parseFieldWithChildrenObject(
      Document document,
      List<Map<String, Object>> fieldValues,
      List<List<String>> facetHierarchyPaths) {
    for (Map.Entry<String, IndexableFieldDef> childField : this.getChildFields().entrySet()) {
      String[] keys = childField.getKey().split("\\.");
      String key = keys[keys.length - 1];
      if (childField.getValue().getType().equals("OBJECT")) {
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
              for (Object e : (List<Object>) childValue) {
                if (e instanceof List || e instanceof Map) {
                  childrenValues.add(gson.toJson(e));
                } else {
                  childrenValues.add(String.valueOf(e));
                }
              }
            } else {
              childrenValues.add(String.valueOf(childValue));
            }
          }
        }
        childField.getValue().parseFieldWithChildren(document, childrenValues, facetHierarchyPaths);
      }
    }
  }

  public boolean isNestedDoc() {
    return isNestedDoc;
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues()) {
      return DocValuesType.BINARY;
    }
    return DocValuesType.NONE;
  }

  @Override
  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.ObjectJsonDocValues(binaryDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  /**
   * wrap list of json string to a single string
   *
   * @param jsonStringList
   * @return
   */
  public static String wrapJsonStringList(List<String> jsonStringList) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(String.join(",", jsonStringList));
    sb.append("]");
    return sb.toString();
  }
}
