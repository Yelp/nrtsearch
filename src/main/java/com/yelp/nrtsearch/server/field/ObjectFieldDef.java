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
package com.yelp.nrtsearch.server.field;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.handler.AddDocumentHandler;
import com.yelp.nrtsearch.server.index.IndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

public class ObjectFieldDef extends IndexableFieldDef<Struct> {

  private final Gson gson;
  private final boolean isNestedDoc;

  protected ObjectFieldDef(String name, Field requestField) {
    super(name, requestField, Struct.class);
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
        document.add(new StoredField(this.getName(), jsonToStruct(fieldValue).toByteArray()));
      }
    }
    if (hasDocValues()) {
      document.add(
          new BinaryDocValuesField(
              getName(), new BytesRef(jsonToStructList(fieldValues).toByteArray())));
    }
    parseFieldWithChildrenObject(document, fieldValueMaps, facetHierarchyPaths);
  }

  public void parseFieldWithChildrenObject(
      Document document,
      List<Map<String, Object>> fieldValues,
      List<List<String>> facetHierarchyPaths) {
    for (Map.Entry<String, IndexableFieldDef<?>> childField : this.getChildFields().entrySet()) {
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
  public LoadedDocValues<Struct> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new LoadedDocValues.ObjectStructDocValues(binaryDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }

  @Override
  public SearchResponse.Hit.FieldValue getStoredFieldValue(StoredValue value) {
    Struct struct = bytesRefToStruct(value.getBinaryValue());
    return SearchResponse.Hit.FieldValue.newBuilder().setStructValue(struct).build();
  }

  /**
   * Convert list of json object strings to protobuf {@link ListValue} of {@link Struct} values.
   *
   * @param jsonStringList list of json strings
   * @return protobuf list of struct values
   */
  public static ListValue jsonToStructList(List<String> jsonStringList) {
    ListValue.Builder listValueBuilder = ListValue.newBuilder();
    for (String jsonString : jsonStringList) {
      listValueBuilder.addValues(
          Value.newBuilder().setStructValue(jsonToStruct(jsonString)).build());
    }
    return listValueBuilder.build();
  }

  /**
   * Convert json object string to protobuf {@link Struct}.
   *
   * @param jsonString json string
   * @return protobuf struct
   */
  public static Struct jsonToStruct(String jsonString) {
    Struct.Builder structBuilder = Struct.newBuilder();
    try {
      JsonFormat.parser().merge(jsonString, structBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return structBuilder.build();
  }

  /**
   * Parse {@link BytesRef} containing a serialized {@link Struct}.
   *
   * @param bytesRef bytes ref
   * @return parsed struct
   */
  public static Struct bytesRefToStruct(BytesRef bytesRef) {
    try {
      return Struct.parser().parseFrom(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
