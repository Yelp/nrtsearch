/*
 * Copyright 2022 Yelp Inc.
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
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues.SingleVector;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

public class VectorFieldDef extends IndexableFieldDef {

  private final int vectorDimensions;
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  /**
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected VectorFieldDef(String name, Field requestField) {
    super(name, requestField);
    this.vectorDimensions = requestField.getVectorDimensions();
  }

  public int getVectorDimensions() {
    return vectorDimensions;
  }

  @Override
  protected void validateRequest(Field requestField) {
    if (requestField.getStore()) {
      throw new IllegalArgumentException("Vector fields cannot be stored");
    }

    if (requestField.getSearch()) {
      throw new IllegalArgumentException("Vector fields cannot be searched");
    }

    if (requestField.getMultiValued()) {
      throw new IllegalArgumentException("Vector fields cannot be multivalued");
    }

    if (requestField.getVectorDimensions() <= 0) {
      throw new IllegalArgumentException("Vector dimension should be > 0");
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getStoreDocValues()) {
      return DocValuesType.BINARY;
    }
    return DocValuesType.NONE;
  }

  @Override
  public String getType() {
    return "VECTOR";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    } else if (fieldValues.size() == 1) {
      if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
        float[] floatArr = parseVectorFieldToFloatArr(fieldValues.get(0));
        byte[] floatBytes = convertFloatArrToBytes(floatArr);
        document.add(new BinaryDocValuesField(getName(), new BytesRef(floatBytes)));
      }
    }
  }

  /**
   * Parses a vector type json string and returns to float[]
   *
   * @param fieldValueJson string to convert to float[]. Ex:"[0.1,0.2]"
   * @return float[] arr represented by input field json
   * @throws IllegalArgumentException if size of vector does not match vector dimensions field
   *     property
   */
  protected float[] parseVectorFieldToFloatArr(String fieldValueJson) {
    float[] fieldValue = GSON.fromJson(fieldValueJson, float[].class);
    if (fieldValue.length != getVectorDimensions()) {
      throw new IllegalArgumentException(
          "The size of the vector data: "
              + fieldValue.length
              + " should match vectorDimensions field property: "
              + getVectorDimensions());
    }
    return fieldValue;
  }

  /**
   * Convert float array into byte array
   *
   * @param floatArr float[] representing vector field values
   * @return byte[] of the input vector field value
   */
  protected static byte[] convertFloatArrToBytes(float[] floatArr) {
    ByteBuffer floatBuffer = ByteBuffer.allocate(Float.BYTES * floatArr.length);
    floatBuffer.asFloatBuffer().put(floatArr);
    return floatBuffer.array();
  }

  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new SingleVector(binaryDocValues);
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }
}
