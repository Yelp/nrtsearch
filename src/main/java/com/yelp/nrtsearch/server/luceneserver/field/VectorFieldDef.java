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

import com.yelp.nrtsearch.server.grpc.Field;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

/** VectorFieldDef extends IndexableFieldDef to add vector type data to the index */
public class VectorFieldDef extends IndexableFieldDef {

  private final int vectorDimensions;

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
      throw new IllegalArgumentException("vector fields cannot be stored");
    }

    if (requestField.getSearch()) {
      throw new IllegalArgumentException("vector fields cannot be searched");
    }
  }

  @Override
  protected DocValuesType parseDocValuesType(Field requestField) {
    if (requestField.getMultiValued()) {
      throw new IllegalArgumentException("Multivalue vectors are not supported");
    } else {
      return DocValuesType.BINARY;
    }
  }

  @Override
  public String getType() {
    return "VECTOR";
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    for (String fieldStr : fieldValues) {
      String value = parseVectorString(fieldStr);
      if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
        document.add(new BinaryDocValuesField(getName(), new BytesRef(value)));
      }
      document.add(new FieldWithData(getName(), fieldType, value));
    }
  }

  /**
   * Parses and evaluated a string list of floats and returns a string Any other value is invalid
   * and will result in an exception.
   *
   * @param vectorString string to convert to list of float. Ex:"[0.1,0.2]"
   * @return string of float represented by input string
   * @throws IllegalArgumentException if input string does not represent a float
   */
  public String parseVectorString(String vectorString) {
    String[] floats = vectorString.replaceAll("^\\[|]$", "").split(",");
    if (floats.length != getVectorDimensions()) {
      throw new IllegalArgumentException(
          "The size of the vector data should match vectorDimensions field property");
    } else {
      try {
        return Arrays.stream(floats).map(Float::parseFloat).toString();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Vector data entry <" + vectorString + "> is malformed. " + e.getMessage());
      }
    }
  }
}
