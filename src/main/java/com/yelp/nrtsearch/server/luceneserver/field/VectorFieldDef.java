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
import com.yelp.nrtsearch.server.grpc.VectorIndexingOptions;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues.SingleSearchVector;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues.SingleVector;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;

public class VectorFieldDef extends IndexableFieldDef {
  private static final Map<String, VectorSimilarityFunction> SIMILARITY_FUNCTION_MAP =
      Map.of(
          "l2_norm",
          VectorSimilarityFunction.EUCLIDEAN,
          "dot_product",
          VectorSimilarityFunction.DOT_PRODUCT,
          "cosine",
          VectorSimilarityFunction.COSINE);
  private static final String HNSW_FORMAT_TYPE = "hnsw";
  private static final int MAX_DOC_VALUE_DIMENSIONS = 2048;
  private final int vectorDimensions;
  private final VectorSimilarityFunction similarityFunction;
  private final KnnVectorsFormat vectorsFormat;
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  private static VectorSimilarityFunction getSimilarityFunction(String vectorSimilarity) {
    VectorSimilarityFunction similarityFunction = SIMILARITY_FUNCTION_MAP.get(vectorSimilarity);
    if (similarityFunction == null) {
      throw new IllegalArgumentException(
          "Unexpected vector similarity \""
              + vectorSimilarity
              + "\", expected one of: "
              + SIMILARITY_FUNCTION_MAP.keySet());
    }
    return similarityFunction;
  }

  private static KnnVectorsFormat createVectorsFormat(VectorIndexingOptions vectorIndexingOptions) {
    if (!HNSW_FORMAT_TYPE.equals(vectorIndexingOptions.getType())) {
      throw new IllegalArgumentException(
          "Unexpected vector format type \""
              + vectorIndexingOptions.getType()
              + "\", expected: "
              + HNSW_FORMAT_TYPE);
    }
    int m =
        vectorIndexingOptions.getHnswM() > 0
            ? vectorIndexingOptions.getHnswM()
            : Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
    int efConstruction =
        vectorIndexingOptions.getHnswEfConstruction() > 0
            ? vectorIndexingOptions.getHnswEfConstruction()
            : Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
    return new Lucene99HnswVectorsFormat(m, efConstruction);
  }

  /**
   * @param name name of field
   * @param requestField field definition from grpc request
   */
  protected VectorFieldDef(String name, Field requestField) {
    super(name, requestField);
    this.vectorDimensions = requestField.getVectorDimensions();
    if (isSearchable()) {
      this.similarityFunction = getSimilarityFunction(requestField.getVectorSimilarity());
      this.vectorsFormat =
          requestField.hasVectorIndexingOptions()
              ? createVectorsFormat(requestField.getVectorIndexingOptions())
              : null;
    } else {
      this.similarityFunction = null;
      this.vectorsFormat = null;
    }
  }

  @Override
  public boolean hasDocValues() {
    // vector data can be loaded out of the index when search is enabled
    return docValuesType != DocValuesType.NONE || isSearchable();
  }

  public int getVectorDimensions() {
    return vectorDimensions;
  }

  @Override
  protected void validateRequest(Field requestField) {
    if (requestField.getStore()) {
      throw new IllegalArgumentException("Vector fields cannot be stored");
    }

    if (requestField.getMultiValued()) {
      throw new IllegalArgumentException("Vector fields cannot be multivalued");
    }

    if (requestField.getVectorDimensions() <= 0) {
      throw new IllegalArgumentException("Vector dimension should be > 0");
    }
    if (requestField.getStoreDocValues()
        && requestField.getVectorDimensions() > MAX_DOC_VALUE_DIMENSIONS) {
      throw new IllegalArgumentException(
          "Vector dimension must be <= " + MAX_DOC_VALUE_DIMENSIONS + " for doc values");
    }

    if (requestField.getSearch()) {
      if (requestField.getVectorDimensions() > Lucene99HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS) {
        throw new IllegalArgumentException(
            "Vector dimension must be <= "
                + Lucene99HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS
                + " for search");
      }
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

  /**
   * Get the format implementation to use for writing vector search data to the index, or null to
   * use the codec default.
   */
  public KnnVectorsFormat getVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public void parseDocumentField(
      Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    float[] floatArr = null;
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    } else if (fieldValues.size() == 1) {
      if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
        floatArr = parseVectorFieldToFloatArr(fieldValues.get(0));
        byte[] floatBytes = convertFloatArrToBytes(floatArr);
        document.add(new BinaryDocValuesField(getName(), new BytesRef(floatBytes)));
      }
      if (isSearchable()) {
        if (floatArr == null) {
          floatArr = parseVectorFieldToFloatArr(fieldValues.get(0));
        }
        validateVectorForSearch(floatArr);
        document.add(new KnnFloatVectorField(getName(), floatArr, similarityFunction));
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

  /**
   * Check vector to ensure it is usable for vector search. Vector must not contain NaN or infinity.
   * When using cosine similarity, vector magnitude cannot be 0. When using dot product similarity,
   * vectors must be normalized to unit length.
   *
   * @param vector vector to check
   * @throws IllegalArgumentException if validation fails
   */
  public void validateVectorForSearch(float[] vector) {
    float magnitude2 = 0;
    for (int i = 0; i < vector.length; ++i) {
      float v = vector[i];
      if (Float.isNaN(v)) {
        throw new IllegalArgumentException("Vector component cannot be NaN");
      }
      if (Float.isInfinite(v)) {
        throw new IllegalArgumentException("Vector component cannot be Infinite");
      }
      magnitude2 += v * v;
    }
    if (similarityFunction == VectorSimilarityFunction.COSINE && magnitude2 == 0.0f) {
      throw new IllegalArgumentException(
          "Vector magnitude cannot be 0 when using cosine similarity");
    }
    if (similarityFunction == VectorSimilarityFunction.DOT_PRODUCT && magnitude2 - 1.0f > 0.0001f) {
      throw new IllegalArgumentException(
          "Vector must be normalized when using dot product similarity");
    }
  }

  public LoadedDocValues<?> getDocValues(LeafReaderContext context) throws IOException {
    if (docValuesType == DocValuesType.BINARY) {
      BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
      return new SingleVector(binaryDocValues);
    } else if (isSearchable()) {
      // fallback to search indexed data if no doc values
      return new SingleSearchVector(context.reader().getFloatVectorValues(getName()));
    }
    throw new IllegalStateException(
        String.format("Unsupported doc value type %s for field %s", docValuesType, this.getName()));
  }
}
