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
package com.yelp.nrtsearch.server.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.doc.LoadedDocValues.SingleSearchVector;
import com.yelp.nrtsearch.server.doc.LoadedDocValues.SingleVector;
import com.yelp.nrtsearch.server.field.properties.VectorQueryable;
import com.yelp.nrtsearch.server.grpc.ExactVectorQuery;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.VectorIndexingOptions;
import com.yelp.nrtsearch.server.query.vector.NrtDiversifyingChildrenByteKnnVectorQuery;
import com.yelp.nrtsearch.server.query.vector.NrtDiversifyingChildrenFloatKnnVectorQuery;
import com.yelp.nrtsearch.server.query.vector.NrtKnnByteVectorQuery;
import com.yelp.nrtsearch.server.query.vector.NrtKnnFloatVectorQuery;
import com.yelp.nrtsearch.server.vector.ByteVectorType;
import com.yelp.nrtsearch.server.vector.FloatVectorType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene102.Lucene102HnswBinaryQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;

/**
 * Field class for 'VECTOR' field type.
 *
 * @param <T> doc value object type
 */
public abstract class VectorFieldDef<T> extends IndexableFieldDef<T> implements VectorQueryable {
  static final int NUM_CANDIDATES_LIMIT = 10000;
  static final String NORMALIZED_COSINE = "normalized_cosine";
  static final String MAGNITUDE_FIELD_SUFFIX = "._magnitude";
  private static final Map<String, VectorSimilarityFunction> SIMILARITY_FUNCTION_MAP =
      Map.of(
          "l2_norm",
          VectorSimilarityFunction.EUCLIDEAN,
          "dot_product",
          VectorSimilarityFunction.DOT_PRODUCT,
          "cosine",
          VectorSimilarityFunction.COSINE,
          NORMALIZED_COSINE,
          VectorSimilarityFunction.DOT_PRODUCT,
          "max_inner_product",
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
  private static final Map<String, VectorSearchType> VECTOR_SEARCH_TYPE_MAP =
      Map.of(
          "hnsw",
          VectorSearchType.HNSW,
          "hnsw_scalar_quantized",
          VectorSearchType.HNSW_SCALAR_QUANTIZED,
          "hnsw_binary_quantized",
          VectorSearchType.HNSW_BINARY_QUANTIZED);
  private static final String DEFAULT_SEARCH_TYPE = "hnsw";
  private static final int MAX_VECTOR_DIMENSIONS = 4096;
  private static final int DEFAULT_QUANTIZED_BITS = 7;
  protected final int vectorDimensions;
  protected final VectorSimilarityFunction similarityFunction;
  private final KnnVectorsFormat vectorsFormat;
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  protected FloatFieldDef magnitudeField;
  private Map<String, IndexableFieldDef<?>> childFieldsWithMagnitude;

  enum VectorSearchType {
    HNSW,
    HNSW_SCALAR_QUANTIZED,
    HNSW_BINARY_QUANTIZED
  }

  /**
   * Get the vector search type for this field.
   *
   * @param vectorIndexingOptions vector indexing options
   * @return vector search type
   */
  abstract VectorSearchType getSearchType(VectorIndexingOptions vectorIndexingOptions);

  /**
   * Parse a vector field value and add it to the document.
   *
   * @param value vector field value
   * @param document document to add field to
   */
  abstract void parseVectorField(String value, Document document);

  /**
   * Get the knn query for this field.
   *
   * @param knnQuery knn query definition
   * @param k number of nearest neighbors to return
   * @param filterQuery filter query
   * @param numCandidates number of candidates to search per segment
   * @param parentBitSetProducer parent bit set producer for searching nested fields, or null
   * @return knn query
   */
  abstract Query getTypeKnnQuery(
      KnnQuery knnQuery,
      int k,
      Query filterQuery,
      int numCandidates,
      BitSetProducer parentBitSetProducer);

  /**
   * Get the exact search query for this field.
   *
   * @param exactVectorQuery exact vector query definition
   * @return exact search query
   */
  abstract Query getTypeExactQuery(ExactVectorQuery exactVectorQuery);

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

  private static KnnVectorsFormat createVectorsFormat(
      VectorSearchType vectorSearchType, VectorIndexingOptions vectorIndexingOptions) {
    int m =
        vectorIndexingOptions.hasHnswM()
            ? vectorIndexingOptions.getHnswM()
            : Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
    int efConstruction =
        vectorIndexingOptions.hasHnswEfConstruction()
            ? vectorIndexingOptions.getHnswEfConstruction()
            : Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
    int mergeWorkers =
        vectorIndexingOptions.hasMergeWorkers() ? vectorIndexingOptions.getMergeWorkers() : 1;
    ExecutorService executorService =
        mergeWorkers > 1
            ? ExecutorFactory.getInstance().getExecutor(ExecutorFactory.ExecutorType.VECTORMERGE)
            : null;
    KnnVectorsFormat vectorsFormat =
        switch (vectorSearchType) {
          case HNSW -> getHnswVectorsFormat(m, efConstruction, mergeWorkers, executorService);
          case HNSW_SCALAR_QUANTIZED ->
              getHnswScalarQuantizedVectorsFormat(
                  m, efConstruction, mergeWorkers, executorService, vectorIndexingOptions);
          case HNSW_BINARY_QUANTIZED ->
              getHnswBinaryQuantizedVectorsFormat(m, efConstruction, mergeWorkers, executorService);
        };

    return new KnnVectorsFormat(vectorsFormat.getName()) {
      @Override
      public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return vectorsFormat.fieldsWriter(state);
      }

      @Override
      public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return vectorsFormat.fieldsReader(state);
      }

      @Override
      public int getMaxDimensions(String fieldName) {
        return MAX_VECTOR_DIMENSIONS;
      }

      @Override
      public String toString() {
        return vectorsFormat.toString();
      }
    };
  }

  private static KnnVectorsFormat getHnswVectorsFormat(
      int m, int efConstruction, int mergeWorkers, ExecutorService executorService) {
    return new Lucene99HnswVectorsFormat(m, efConstruction, mergeWorkers, executorService);
  }

  private static KnnVectorsFormat getHnswScalarQuantizedVectorsFormat(
      int m,
      int efConstruction,
      int mergeWorkers,
      ExecutorService executorService,
      VectorIndexingOptions vectorIndexingOptions) {
    Float confidenceInterval =
        vectorIndexingOptions.hasQuantizedConfidenceInterval()
            ? vectorIndexingOptions.getQuantizedConfidenceInterval()
            : null;
    int bits =
        vectorIndexingOptions.hasQuantizedBits()
            ? vectorIndexingOptions.getQuantizedBits()
            : DEFAULT_QUANTIZED_BITS;
    boolean compress =
        vectorIndexingOptions.hasQuantizedCompress()
            && vectorIndexingOptions.getQuantizedCompress();

    return new Lucene99HnswScalarQuantizedVectorsFormat(
        m, efConstruction, mergeWorkers, bits, compress, confidenceInterval, executorService);
  }

  private static KnnVectorsFormat getHnswBinaryQuantizedVectorsFormat(
      int m, int efConstruction, int mergeWorkers, ExecutorService executorService) {
    return new Lucene102HnswBinaryQuantizedVectorsFormat(
        m, efConstruction, mergeWorkers, executorService);
  }

  @VisibleForTesting
  static KnnSearchStrategy createKnnSearchStrategy(KnnQuery.FilterStrategy filterStrategy) {
    return switch (filterStrategy) {
      case FANOUT -> new KnnSearchStrategy.Hnsw(0);
      case ACORN -> new KnnSearchStrategy.Hnsw(60);
      default -> throw new IllegalArgumentException("Unknown filter strategy: " + filterStrategy);
    };
  }

  /**
   * Create a new VectorFieldDef from a grpc Field definition.
   *
   * @param name name of field
   * @param field field definition from grpc request
   * @param context creation context
   * @return new VectorFieldDef
   */
  static VectorFieldDef<?> createField(
      String name, Field field, FieldDefCreator.FieldDefCreatorContext context) {
    return switch (field.getVectorElementType()) {
      case VECTOR_ELEMENT_FLOAT -> new FloatVectorFieldDef(name, field, context);
      case VECTOR_ELEMENT_BYTE -> new ByteVectorFieldDef(name, field, context);
      default ->
          throw new IllegalArgumentException("Invalid field type: " + field.getVectorElementType());
    };
  }

  /**
   * @param name name of field
   * @param requestField field definition from grpc request
   * @param context creation context
   * @param docValuesClass class of doc values object
   */
  protected VectorFieldDef(
      String name,
      Field requestField,
      FieldDefCreator.FieldDefCreatorContext context,
      Class<T> docValuesClass) {
    super(name, requestField, context, docValuesClass);
    this.vectorDimensions = requestField.getVectorDimensions();
    if (isSearchable()) {
      VectorSearchType vectorSearchType = getSearchType(requestField.getVectorIndexingOptions());
      this.similarityFunction = getSimilarityFunction(requestField.getVectorSimilarity());
      setupNormalizedVectorField(requestField.getVectorSimilarity(), context);
      this.vectorsFormat =
          createVectorsFormat(vectorSearchType, requestField.getVectorIndexingOptions());
    } else {
      this.similarityFunction = null;
      this.vectorsFormat = null;
      childFieldsWithMagnitude = super.getChildFields();
    }
  }

  private void setupNormalizedVectorField(
      String similarity, FieldDefCreator.FieldDefCreatorContext context) {
    if (NORMALIZED_COSINE.equals(similarity)) {
      // add field to store magnitude before normalization
      magnitudeField =
          new FloatFieldDef(
              getName() + MAGNITUDE_FIELD_SUFFIX,
              Field.newBuilder()
                  .setName(getName() + MAGNITUDE_FIELD_SUFFIX)
                  .setType(FieldType.FLOAT)
                  .setStoreDocValues(true)
                  .build(),
              context);
      Map<String, IndexableFieldDef<?>> childFieldsMap = new HashMap<>(super.getChildFields());
      childFieldsMap.put(magnitudeField.getName(), magnitudeField);
      childFieldsWithMagnitude = Collections.unmodifiableMap(childFieldsMap);
    } else {
      childFieldsWithMagnitude = super.getChildFields();
    }
  }

  @Override
  public Map<String, IndexableFieldDef<?>> getChildFields() {
    return childFieldsWithMagnitude;
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
    if ((requestField.getStoreDocValues() || requestField.getSearch())
        && requestField.getVectorDimensions() > MAX_VECTOR_DIMENSIONS) {
      throw new IllegalArgumentException("Vector dimension must be <= " + MAX_VECTOR_DIMENSIONS);
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
    if (fieldValues.size() > 1 && !isMultiValue()) {
      throw new IllegalArgumentException(
          "Cannot index multiple values into single value field: " + getName());
    } else if (fieldValues.size() == 1) {
      parseVectorField(fieldValues.getFirst(), document);
    }
  }

  @Override
  public Query getKnnQuery(
      KnnQuery knnQuery, Query filterQuery, BitSetProducer parentBitSetProducer) {
    if (!isSearchable()) {
      throw new IllegalArgumentException("Vector field is not searchable: " + getName());
    }

    int k = knnQuery.getK();
    int numCandidates = knnQuery.getNumCandidates();
    if (k < 1) {
      throw new IllegalArgumentException("Vector search k must be >= 1");
    }
    if (numCandidates < k) {
      throw new IllegalArgumentException("Vector search numCandidates must be >= k");
    }
    if (numCandidates > NUM_CANDIDATES_LIMIT) {
      throw new IllegalArgumentException("Vector search numCandidates > " + NUM_CANDIDATES_LIMIT);
    }

    return getTypeKnnQuery(knnQuery, k, filterQuery, numCandidates, parentBitSetProducer);
  }

  @Override
  public Query getExactQuery(ExactVectorQuery exactVectorQuery) {
    if (!isSearchable()) {
      throw new IllegalArgumentException("Vector field is not searchable: " + getName());
    }
    return getTypeExactQuery(exactVectorQuery);
  }

  /** Field class for 'FLOAT' vector field type. */
  public static class FloatVectorFieldDef extends VectorFieldDef<FloatVectorType> {
    public FloatVectorFieldDef(
        String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
      super(name, requestField, context, FloatVectorType.class);
    }

    @Override
    public LoadedDocValues<FloatVectorType> getDocValues(LeafReaderContext context)
        throws IOException {
      if (docValuesType == DocValuesType.BINARY) {
        BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
        return new SingleVector(binaryDocValues);
      } else if (isSearchable()) {
        // fallback to search indexed data if no doc values
        return new SingleSearchVector(context.reader().getFloatVectorValues(getName()));
      }
      throw new IllegalStateException(
          String.format(
              "Unsupported doc value type %s for field %s", docValuesType, this.getName()));
    }

    @Override
    VectorSearchType getSearchType(VectorIndexingOptions vectorIndexingOptions) {
      String searchType =
          vectorIndexingOptions.hasType() ? vectorIndexingOptions.getType() : DEFAULT_SEARCH_TYPE;
      VectorSearchType vectorSearchType = VECTOR_SEARCH_TYPE_MAP.get(searchType);
      if (vectorSearchType == null) {
        throw new IllegalArgumentException(
            "Unexpected vector search type \""
                + searchType
                + "\", expected one of: "
                + VECTOR_SEARCH_TYPE_MAP.keySet());
      }
      if (vectorSearchType == VectorSearchType.HNSW_SCALAR_QUANTIZED
          && vectorIndexingOptions.getQuantizedBits() == 4
          && (vectorDimensions % 2) != 0) {
        throw new IllegalArgumentException(
            "HNSW scalar quantized search type with 4 bits requires vector dimensions to be a multiple of 2");
      }
      return vectorSearchType;
    }

    @Override
    void parseVectorField(String value, Document document) {
      float[] floatArr = null;
      if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
        floatArr = parseVectorFieldToFloatArr(value);
        byte[] floatBytes = convertFloatArrToBytes(floatArr);
        document.add(new BinaryDocValuesField(getName(), new BytesRef(floatBytes)));
      }
      if (isSearchable()) {
        if (floatArr == null) {
          floatArr = parseVectorFieldToFloatArr(value);
        }
        float magnitude2 = validateVectorForSearch(floatArr);
        if (magnitudeField != null) {
          float magnitude = (float) Math.sqrt(magnitude2);
          normalizeVector(floatArr, magnitude);
          magnitudeField.parseDocumentField(document, List.of(String.valueOf(magnitude)), null);
        }
        document.add(new KnnFloatVectorField(getName(), floatArr, similarityFunction));
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
    float[] parseVectorFieldToFloatArr(String fieldValueJson) {
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
    static byte[] convertFloatArrToBytes(float[] floatArr) {
      ByteBuffer floatBuffer = ByteBuffer.allocate(Float.BYTES * floatArr.length);
      floatBuffer.asFloatBuffer().put(floatArr);
      return floatBuffer.array();
    }

    @Override
    Query getTypeKnnQuery(
        KnnQuery knnQuery,
        int k,
        Query filterQuery,
        int numCandidates,
        BitSetProducer parentBitSetProducer) {
      if (knnQuery.getQueryVectorCount() != getVectorDimensions()) {
        throw new IllegalArgumentException(
            "Invalid query vector size, expected: "
                + getVectorDimensions()
                + ", found: "
                + knnQuery.getQueryVectorCount());
      }
      float[] queryVector = new float[knnQuery.getQueryVectorCount()];
      for (int i = 0; i < knnQuery.getQueryVectorCount(); ++i) {
        queryVector[i] = knnQuery.getQueryVector(i);
      }
      float magnitude2 = validateVectorForSearch(queryVector);
      // If using normalized cosine similarity, normalize the query vector
      if (magnitudeField != null) {
        float magnitude = (float) Math.sqrt(magnitude2);
        normalizeVector(queryVector, magnitude);
      }
      KnnSearchStrategy searchStrategy = createKnnSearchStrategy(knnQuery.getFilterStrategy());
      if (parentBitSetProducer != null) {
        return new NrtDiversifyingChildrenFloatKnnVectorQuery(
            getName(),
            queryVector,
            filterQuery,
            k,
            numCandidates,
            parentBitSetProducer,
            searchStrategy);
      } else {
        return new NrtKnnFloatVectorQuery(
            getName(), queryVector, k, filterQuery, numCandidates, searchStrategy);
      }
    }

    @Override
    public Query getTypeExactQuery(ExactVectorQuery exactVectorQuery) {
      if (exactVectorQuery.getQueryFloatVectorCount() != getVectorDimensions()) {
        throw new IllegalArgumentException(
            "Invalid query float vector size, expected: "
                + getVectorDimensions()
                + ", found: "
                + exactVectorQuery.getQueryFloatVectorCount());
      }
      float[] queryVector = new float[exactVectorQuery.getQueryFloatVectorCount()];
      for (int i = 0; i < exactVectorQuery.getQueryFloatVectorCount(); ++i) {
        queryVector[i] = exactVectorQuery.getQueryFloatVector(i);
      }
      float magnitude2 = validateVectorForSearch(queryVector);
      // If using normalized cosine similarity, normalize the query vector
      if (magnitudeField != null) {
        float magnitude = (float) Math.sqrt(magnitude2);
        normalizeVector(queryVector, magnitude);
      }
      return new com.yelp.nrtsearch.server.query.vector.ExactVectorQuery.ExactFloatVectorQuery(
          getName(), queryVector);
    }

    /**
     * Check vector to ensure it is usable for vector search. Vector must not contain NaN or
     * infinity. When using cosine similarity, vector magnitude cannot be 0. When using dot product
     * similarity, vectors must be normalized to unit length.
     *
     * @param vector vector to check
     * @return magnitude squared of the vector
     * @throws IllegalArgumentException if validation fails
     */
    public float validateVectorForSearch(float[] vector) {
      VectorUtil.checkFinite(vector);
      float magnitude2 = VectorUtil.dotProduct(vector, vector);
      // cosine or normalized_cosine
      if ((similarityFunction == VectorSimilarityFunction.COSINE
              || (similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                  && magnitudeField != null))
          && magnitude2 == 0.0f) {
        throw new IllegalArgumentException(
            "Vector magnitude cannot be 0 when using cosine similarity");
      }
      // dot_product, but not normalized_cosine
      if (similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
          && magnitude2 - 1.0f > 0.0001f
          && magnitudeField == null) {
        throw new IllegalArgumentException(
            "Vector must be normalized when using dot product similarity");
      }
      return magnitude2;
    }

    private void normalizeVector(float[] vector, float magnitude) {
      for (int i = 0; i < vector.length; i++) {
        vector[i] /= magnitude;
      }
    }
  }

  /** Field class for 'BYTE' vector field type. */
  public static class ByteVectorFieldDef extends VectorFieldDef<ByteVectorType> {
    public ByteVectorFieldDef(
        String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
      super(name, requestField, context, ByteVectorType.class);
      if (NORMALIZED_COSINE.equals(requestField.getVectorSimilarity())) {
        throw new IllegalArgumentException(
            "Normalized cosine similarity is not supported for byte vectors");
      }
    }

    @Override
    public LoadedDocValues<ByteVectorType> getDocValues(LeafReaderContext context)
        throws IOException {
      if (docValuesType == DocValuesType.BINARY) {
        BinaryDocValues binaryDocValues = DocValues.getBinary(context.reader(), getName());
        return new LoadedDocValues.SingleByteVector(binaryDocValues);
      } else if (isSearchable()) {
        // fallback to search indexed data if no doc values
        return new LoadedDocValues.SingleSearchByteVector(
            context.reader().getByteVectorValues(getName()));
      }
      throw new IllegalStateException(
          String.format(
              "Unsupported doc value type %s for field %s", docValuesType, this.getName()));
    }

    @Override
    VectorSearchType getSearchType(VectorIndexingOptions vectorIndexingOptions) {
      String searchType =
          vectorIndexingOptions.hasType() ? vectorIndexingOptions.getType() : DEFAULT_SEARCH_TYPE;
      VectorSearchType vectorSearchType = VECTOR_SEARCH_TYPE_MAP.get(searchType);
      if (vectorSearchType == null) {
        throw new IllegalArgumentException(
            "Unexpected vector search type \""
                + searchType
                + "\", expected one of: "
                + VECTOR_SEARCH_TYPE_MAP.keySet());
      }
      if (vectorSearchType == VectorSearchType.HNSW_SCALAR_QUANTIZED) {
        throw new IllegalArgumentException(
            "HNSW scalar quantized search type is only supported for float vectors");
      }
      if (vectorSearchType == VectorSearchType.HNSW_BINARY_QUANTIZED) {
        throw new IllegalArgumentException(
            "HNSW binary quantized search type is only supported for float vectors");
      }
      return vectorSearchType;
    }

    @Override
    void parseVectorField(String value, Document document) {
      byte[] byteArr = null;
      if (hasDocValues() && docValuesType == DocValuesType.BINARY) {
        byteArr = parseVectorFieldToByteArr(value);
        document.add(new BinaryDocValuesField(getName(), new BytesRef(byteArr)));
      }
      if (isSearchable()) {
        if (byteArr == null) {
          byteArr = parseVectorFieldToByteArr(value);
        }
        validateVectorForSearch(byteArr);
        document.add(new KnnByteVectorField(getName(), byteArr, similarityFunction));
      }
    }

    /**
     * Parses a vector type json string and returns to byte[]
     *
     * @param fieldValueJson string to convert to byte[]. Ex:"[-10,0.100]"
     * @return byte[] arr represented by input field json
     * @throws IllegalArgumentException if size of vector does not match vector dimensions field
     *     property, or if a value is not an integer or out of byte range
     */
    byte[] parseVectorFieldToByteArr(String fieldValueJson) {
      float[] fieldValueFloat = GSON.fromJson(fieldValueJson, float[].class);
      if (fieldValueFloat.length != getVectorDimensions()) {
        throw new IllegalArgumentException(
            "The size of the vector data: "
                + fieldValueFloat.length
                + " should match vectorDimensions field property: "
                + getVectorDimensions());
      }
      byte[] fieldValue = new byte[fieldValueFloat.length];
      for (int i = 0; i < fieldValueFloat.length; i++) {
        float value = fieldValueFloat[i];
        if (value % 1 != 0) {
          throw new IllegalArgumentException(
              "Byte value is not an integer: " + value + " at index: " + i);
        }
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
          throw new IllegalArgumentException(
              "Byte value out of range: " + (int) value + " at index: " + i);
        }
        fieldValue[i] = (byte) value;
      }
      return fieldValue;
    }

    @Override
    Query getTypeKnnQuery(
        KnnQuery knnQuery,
        int k,
        Query filterQuery,
        int numCandidates,
        BitSetProducer parentBitSetProducer) {
      if (knnQuery.getQueryByteVector().size() != getVectorDimensions()) {
        throw new IllegalArgumentException(
            "Invalid query byte vector size, expected: "
                + getVectorDimensions()
                + ", found: "
                + knnQuery.getQueryByteVector().size());
      }
      byte[] queryVector = knnQuery.getQueryByteVector().toByteArray();
      validateVectorForSearch(queryVector);
      KnnSearchStrategy searchStrategy = createKnnSearchStrategy(knnQuery.getFilterStrategy());
      if (parentBitSetProducer != null) {
        return new NrtDiversifyingChildrenByteKnnVectorQuery(
            getName(),
            queryVector,
            filterQuery,
            k,
            numCandidates,
            parentBitSetProducer,
            searchStrategy);
      } else {
        return new NrtKnnByteVectorQuery(
            getName(), queryVector, k, filterQuery, numCandidates, searchStrategy);
      }
    }

    @Override
    public Query getTypeExactQuery(ExactVectorQuery exactVectorQuery) {
      if (exactVectorQuery.getQueryByteVector().size() != getVectorDimensions()) {
        throw new IllegalArgumentException(
            "Invalid query byte vector size, expected: "
                + getVectorDimensions()
                + ", found: "
                + exactVectorQuery.getQueryByteVector().size());
      }
      byte[] queryVector = exactVectorQuery.getQueryByteVector().toByteArray();
      validateVectorForSearch(queryVector);
      return new com.yelp.nrtsearch.server.query.vector.ExactVectorQuery.ExactByteVectorQuery(
          getName(), queryVector);
    }

    /**
     * Check vector to ensure it is usable for vector search. When using cosine similarity, vector
     * magnitude cannot be 0.
     *
     * @param vector vector to check
     * @throws IllegalArgumentException if validation fails
     */
    public void validateVectorForSearch(byte[] vector) {
      if (similarityFunction == VectorSimilarityFunction.COSINE) {
        int magnitude2 = VectorUtil.dotProduct(vector, vector);
        if (magnitude2 == 0) {
          throw new IllegalArgumentException(
              "Vector magnitude cannot be 0 when using cosine similarity");
        }
      }
    }
  }
}
