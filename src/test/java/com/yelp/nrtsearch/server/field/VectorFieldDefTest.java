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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.primitives.Floats;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics.VectorDiagnostics;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue.Vector;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class VectorFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static final String VECTOR_SEARCH_INDEX_NAME = "vector_search_index";
  public static final String NESTED_VECTOR_SEARCH_INDEX_NAME = "nested_vector_search_index";
  private static final String FIELD_NAME = "vector_field";
  private static final String FIELD_TYPE = "VECTOR";
  private static final List<String> VECTOR_FIELD_VALUES =
      Arrays.asList("[1.0, 2.5, 1000.1000]", "[0.1, -2.0, 5.6]");

  @Override
  protected List<String> getIndices() {
    return List.of(DEFAULT_TEST_INDEX, VECTOR_SEARCH_INDEX_NAME, NESTED_VECTOR_SEARCH_INDEX_NAME);
  }

  private Map<String, MultiValuedField> getFieldsMapForOneDocument(String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap = new HashMap<>();
    fieldsMap.put(
        FIELD_NAME, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    return fieldsMap;
  }

  private List<AddDocumentRequest> buildDocuments(
      String indexName, List<String> vectorFieldValues) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : vectorFieldValues) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    return documentRequests;
  }

  private void indexVectorSearchDocs() throws Exception {
    IndexWriter writer =
        getGlobalState().getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME).getShard(0).writer;
    // don't want any merges for these tests to verify parallel search
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    getGlobalState()
        .getIndexStateManagerOrThrow(DEFAULT_TEST_INDEX)
        .updateLiveSettings(
            IndexLiveSettings.newBuilder().setSliceMaxSegments(Int32Value.of(1)).build(), false);

    // make testing deterministic
    Random random = new Random(123456);
    for (int i = 0; i < 10; ++i) {
      List<AddDocumentRequest> docs = new ArrayList<>();

      for (int j = 0; j < 1000; ++j) {
        docs.add(
            AddDocumentRequest.newBuilder()
                .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                .putFields(
                    "vector_l2_norm",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 3, false))
                        .build())
                .putFields(
                    "vector_cosine",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 3, false))
                        .build())
                .putFields(
                    "vector_dot",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 3, true))
                        .build())
                .putFields(
                    "vector_mip",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 3, true))
                        .build())
                .putFields(
                    "byte_vector_l2_norm",
                    MultiValuedField.newBuilder()
                        .addValue(createByteVectorString(random, 3))
                        .build())
                .putFields(
                    "byte_vector_cosine",
                    MultiValuedField.newBuilder()
                        .addValue(createByteVectorString(random, 3))
                        .build())
                .putFields(
                    "byte_vector_dot",
                    MultiValuedField.newBuilder()
                        .addValue(createByteVectorString(random, 3))
                        .build())
                .putFields(
                    "byte_vector_mip",
                    MultiValuedField.newBuilder()
                        .addValue(createByteVectorString(random, 3))
                        .build())
                .putFields(
                    "quantized_vector_4",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 4, false))
                        .build())
                .putFields(
                    "quantized_vector_7",
                    MultiValuedField.newBuilder()
                        .addValue(createVectorString(random, 3, false))
                        .build())
                .putFields(
                    "filter", MultiValuedField.newBuilder().addValue("term" + j % 10).build())
                .build());
      }

      addDocuments(docs.stream());
      writer.flush();
    }
  }

  private void indexNestedVectorSearchDocs() throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
            .putFields("id", MultiValuedField.newBuilder().addValue("0").build())
            .putFields("filter_field", MultiValuedField.newBuilder().addValue("1").build())
            .putFields(
                "nested_object",
                MultiValuedField.newBuilder()
                    .addValue(
                        "{\"float_vector\": \"[0.25, 0.5, 0.1]\", \"byte_vector\": \"[-50, -5, 0]\"}")
                    .addValue(
                        "{\"float_vector\": \"[0.2, 0.4, 0.3]\", \"byte_vector\": \"[-8, -10, -1]\"}")
                    .build())
            .build());
    docs.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
            .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
            .putFields("filter_field", MultiValuedField.newBuilder().addValue("2").build())
            .putFields(
                "nested_object",
                MultiValuedField.newBuilder()
                    .addValue(
                        "{\"float_vector\": \"[0.75, 0.9, 0.6]\", \"byte_vector\": \"[50, 5, 0]\"}")
                    .addValue(
                        "{\"float_vector\": \"[0.7, 0.8, 0.9]\", \"byte_vector\": \"[8, 10, 1]\"}")
                    .build())
            .build());
    addDocuments(docs.stream());
  }

  private String createVectorString(Random random, int size, boolean normalize) {
    List<Float> vector = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      vector.add(random.nextFloat());
    }
    if (normalize) {
      vector = normalizeVector(vector);
    }
    return vector.toString();
  }

  private List<Float> normalizeVector(List<Float> vec) {
    float magnitude = 0;
    for (Float v : vec) {
      magnitude += v * v;
    }
    magnitude = (float) Math.sqrt(magnitude);
    List<Float> normVec = new ArrayList<>(vec.size());
    for (Float v : vec) {
      normVec.add(v / magnitude);
    }
    return normVec;
  }

  private String createByteVectorString(Random random, int size) {
    byte[] vector = new byte[size];
    for (int i = 0; i < size; ++i) {
      vector[i] = (byte) (random.nextInt(256) - 128);
    }
    return Arrays.toString(vector);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    if (DEFAULT_TEST_INDEX.equals(name)) {
      return getFieldsFromResourceFile("/field/registerFieldsVector.json");
    } else if (VECTOR_SEARCH_INDEX_NAME.equals(name)) {
      return getFieldsFromResourceFile("/field/registerFieldsVectorSearch.json");
    } else if (NESTED_VECTOR_SEARCH_INDEX_NAME.equals(name)) {
      return getFieldsFromResourceFile("/field/registerFieldsNestedVectorSearch.json");
    }
    throw new IllegalArgumentException("Unknown index name: " + name);
  }

  @Override
  public void initIndex(String name) throws Exception {
    if (DEFAULT_TEST_INDEX.equals(name)) {
      List<AddDocumentRequest> documents = buildDocuments(name, VECTOR_FIELD_VALUES);
      addDocuments(documents.stream());
    } else if (VECTOR_SEARCH_INDEX_NAME.equals(name)) {
      indexVectorSearchDocs();
    } else if (NESTED_VECTOR_SEARCH_INDEX_NAME.equals(name)) {
      indexNestedVectorSearchDocs();
    } else {
      throw new IllegalArgumentException("Unknown index name: " + name);
    }
  }

  public FieldDef getFieldDef(String testIndex, String fieldName) throws IOException {
    return getGrpcServer().getGlobalState().getIndexOrThrow(testIndex).getFieldOrThrow(fieldName);
  }

  @Test
  public void validVectorFieldDefTest() throws IOException {
    FieldDef vectorFieldDef = getFieldDef(DEFAULT_TEST_INDEX, FIELD_NAME);
    assertEquals(FIELD_TYPE, vectorFieldDef.getType());
    assertEquals(FIELD_NAME, vectorFieldDef.getName());

    float[] expectedVector1 = {1.0f, 2.5f, 1000.1000f};
    float[] expectedVector2 = {0.1f, -2.0f, 5.6f};

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields(FIELD_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());
    Vector fieldValue1 =
        searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    Vector fieldValue2 =
        searchResponse.getHits(1).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    assertEquals(Floats.asList(expectedVector1), fieldValue1.getValueList());
    assertEquals(Floats.asList(expectedVector2), fieldValue2.getValueList());
  }

  @Test
  public void vectorDimensionMismatchTest() {
    List<String> vectorFields = Arrays.asList("[1.0, 2.5]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, vectorFields);
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documents.stream()));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The size of the vector data: 2 should match vectorDimensions field property: 3"));
  }

  @Test
  public void vectorFieldMultiValueExceptionTest() {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .putFields(
                FIELD_NAME,
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addAllValue(VECTOR_FIELD_VALUES)
                    .build())
            .build());
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documentRequests.stream()));
    assertTrue(
        exception
            .getMessage()
            .contains("Cannot index multiple values into single value field: vector_field"));
  }

  @Test
  public void parseVectorFieldToFloatArrTest() throws Exception {
    float[] expected = {1.0f, 2.5f, 1000.1000f};

    List<String> vectorFields = Arrays.asList("[1.0, 2.5, 1000.1000]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, vectorFields);
    addDocuments(documents.stream());

    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields(FIELD_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());
    Vector fieldValue =
        searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getVectorValue();
    assertEquals(Floats.asList(expected), fieldValue.getValueList());
  }

  @Test
  public void parseVectorFieldToFloatArrFailTest() {
    List<String> invalidJsonList = Arrays.asList("[a, b, c]");
    List<AddDocumentRequest> documents = buildDocuments(DEFAULT_TEST_INDEX, invalidJsonList);
    Exception exception =
        Assert.assertThrows(RuntimeException.class, () -> addDocuments(documents.stream()));
    assertTrue(exception.getMessage().contains("For input string: \"a\""));
  }

  @Test
  public void vectorStoreRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_store")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .setStore(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector fields cannot be stored"));
  }

  @Test
  public void vectorMultiValueRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_multi_value")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .setMultiValued(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector fields cannot be multivalued"));
  }

  @Test
  public void vectorInvalidDimensionRequestFailTest() {
    Exception exception =
        Assert.assertThrows(
            RuntimeException.class,
            () ->
                getGrpcServer()
                    .getBlockingStub()
                    .updateFields(
                        FieldDefRequest.newBuilder()
                            .setIndexName(DEFAULT_TEST_INDEX)
                            .addField(
                                Field.newBuilder()
                                    .setName("vector_field_missing_dimensions")
                                    .setType(FieldType.VECTOR)
                                    .setStoreDocValues(true)
                                    .build())
                            .build()));
    assertTrue(exception.getMessage().contains("Vector dimension should be > 0"));
  }

  @Test
  public void testVectorSearch_l2_norm() {
    singleVectorQueryAndVerify(
        "vector_l2_norm", List.of(0.25f, 0.5f, 0.75f), VectorSimilarityFunction.EUCLIDEAN, 1.0f);
  }

  @Test
  public void testVectorSearch_cosine() {
    singleVectorQueryAndVerify(
        "vector_cosine", List.of(0.25f, 0.5f, 0.75f), VectorSimilarityFunction.COSINE, 1.0f);
  }

  @Test
  public void testVectorSearch_normCosine() {
    singleNormCosineQueryAndVerify(List.of(0.25f, 0.5f, 0.75f), 1.0f);
  }

  @Test
  public void testVectorSearch_normCosineDV() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields("vector_cosine")
                    .addRetrieveFields("vector_cosine.normalized_doc_values")
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("vector_cosine.normalized_doc_values")
                            .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(5, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      float[] vector =
          floatListToArray(
              hit.getFieldsOrThrow("vector_cosine")
                  .getFieldValue(0)
                  .getVectorValue()
                  .getValueList());
      float[] vectorDV =
          floatListToArray(
              hit.getFieldsOrThrow("vector_cosine.normalized_doc_values")
                  .getFieldValue(0)
                  .getVectorValue()
                  .getValueList());
      assertArrayEquals(vector, vectorDV, 0.0001f);
    }
  }

  @Test
  public void testVectorSearch_dot() {
    singleVectorQueryAndVerify(
        "vector_dot",
        normalizeVector(List.of(0.25f, 0.5f, 0.75f)),
        VectorSimilarityFunction.DOT_PRODUCT,
        1.0f);
  }

  @Test
  public void testVectorSearch_mip() {
    singleVectorQueryAndVerify(
        "vector_mip",
        List.of(0.25f, 0.5f, 0.75f),
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
        1.0f);
  }

  @Test
  public void testByteVectorSearch_l2_norm() {
    singleByteVectorQueryAndVerify(
        "byte_vector_l2_norm", new byte[] {-50, 5, 100}, VectorSimilarityFunction.EUCLIDEAN, 1.0f);
  }

  @Test
  public void testByteVectorSearch_cosine() {
    singleByteVectorQueryAndVerify(
        "byte_vector_cosine", new byte[] {-50, 5, 100}, VectorSimilarityFunction.COSINE, 1.0f);
  }

  @Test
  public void testByteVectorSearch_dot() {
    singleByteVectorQueryAndVerify(
        "byte_vector_dot", new byte[] {-50, 5, 100}, VectorSimilarityFunction.DOT_PRODUCT, 1.0f);
  }

  @Test
  public void testByteVectorSearch_mip() {
    singleByteVectorQueryAndVerify(
        "byte_vector_mip",
        new byte[] {-50, 5, 100},
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
        1.0f);
  }

  @Test
  public void testQuantizedVectorSearch_4() {
    singleVectorQueryAndVerify(
        "quantized_vector_4",
        List.of(0.25f, 0.5f, 0.75f, 0.1f),
        VectorSimilarityFunction.COSINE,
        1.0f,
        0.01);
  }

  @Test
  public void testQuantizedVectorSearch_7() {
    singleVectorQueryAndVerify(
        "quantized_vector_7",
        List.of(0.25f, 0.5f, 0.75f),
        VectorSimilarityFunction.COSINE,
        1.0f,
        0.001);
  }

  @Test
  public void testQuantizedVectorSearch_7_normCosine() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    singleVectorQueryAndVerify(
        "quantized_vector_7.normalized", queryVector, VectorSimilarityFunction.COSINE, 1.0f, 0.001);
    float magnitude = (float) Math.sqrt(0.25f * 0.25f + 0.5f * 0.5f + 0.75f * 0.75f);
    List<Float> normalizedQueryVector = new ArrayList<>();
    for (Float v : queryVector) {
      normalizedQueryVector.add(v / magnitude);
    }
    singleVectorQueryAndVerify(
        "quantized_vector_7.normalized",
        normalizedQueryVector,
        VectorSimilarityFunction.DOT_PRODUCT,
        1.0f,
        0.001);
  }

  @Test
  public void testVectorSearch_boost() {
    singleVectorQueryAndVerify(
        "vector_l2_norm", List.of(0.25f, 0.5f, 0.75f), VectorSimilarityFunction.EUCLIDEAN, 2.0f);
  }

  @Test
  public void testByteVectorSearch_boost() {
    singleByteVectorQueryAndVerify(
        "byte_vector_l2_norm", new byte[] {-50, 5, 100}, VectorSimilarityFunction.EUCLIDEAN, 2.0f);
  }

  @Test
  public void testVectorSearch_default_boost() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    String field = "vector_cosine";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(5, searchResponse.getHitsCount());
    verifyHitsSimilarity(
        field, queryVector, searchResponse, VectorSimilarityFunction.COSINE, 1.0f, 0.0001);
  }

  @Test
  public void testVectorSearch_index_value_loading() {
    singleVectorQueryAndVerify(
        "vector_l2_norm.no_doc_values",
        List.of(0.25f, 0.5f, 0.75f),
        VectorSimilarityFunction.EUCLIDEAN,
        1.0f);
  }

  @Test
  public void testByteVectorSearch_index_value_loading() {
    singleByteVectorQueryAndVerify(
        "byte_vector_l2_norm.no_doc_values",
        new byte[] {-50, 5, 100},
        VectorSimilarityFunction.EUCLIDEAN,
        1.0f);
  }

  @Test
  public void testVectorSearch_index_value_no_data() {
    String field = "vector_no_data";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setTopHits(3)
                    .build());
    assertEquals(3, searchResponse.getHitsCount());
    assertEquals(0, searchResponse.getHits(0).getFieldsOrThrow(field).getFieldValueCount());
    assertEquals(0, searchResponse.getHits(1).getFieldsOrThrow(field).getFieldValueCount());
    assertEquals(0, searchResponse.getHits(2).getFieldsOrThrow(field).getFieldValueCount());
  }

  @Test
  public void testByteVectorSearch_index_value_no_data() {
    String field = "byte_vector_no_data";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setTopHits(3)
                    .build());
    assertEquals(3, searchResponse.getHitsCount());
    assertEquals(0, searchResponse.getHits(0).getFieldsOrThrow(field).getFieldValueCount());
    assertEquals(0, searchResponse.getHits(1).getFieldsOrThrow(field).getFieldValueCount());
    assertEquals(0, searchResponse.getHits(2).getFieldsOrThrow(field).getFieldValueCount());
  }

  @Test
  public void testVectorSearch_start_hit() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    String field = "vector_cosine";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setStartHit(2)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(3, searchResponse.getHitsCount());
  }

  @Test
  public void testVectorSearch_top_hits() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    String field = "vector_cosine";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setStartHit(0)
                    .setTopHits(4)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(4, searchResponse.getHitsCount());
  }

  @Test
  public void testVectorSearch_filter() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    String field = "vector_cosine";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .addRetrieveFields("filter")
                    .setStartHit(0)
                    .setTopHits(20)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(50)
                            .setK(20)
                            .setFilter(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("filter")
                                            .setTextValue("term2")
                                            .build())
                                    .build())
                            .build())
                    .build());
    assertEquals(20, searchResponse.getHitsCount());
    for (Hit hit : searchResponse.getHitsList()) {
      assertEquals("term2", hit.getFieldsOrThrow("filter").getFieldValue(0).getTextValue());
    }
  }

  @Test
  public void testMultipleVectorSearch() {
    List<Float> queryVector = List.of(0.25f, 0.5f, 0.75f);
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields("vector_cosine")
                    .addRetrieveFields("vector_l2_norm")
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("vector_cosine")
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(2)
                            .setBoost(5)
                            .build())
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("vector_l2_norm")
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertTrue(searchResponse.getHitsCount() >= 5);
    assertTrue(searchResponse.getHits(0).getScore() > 1.0);
    assertTrue(searchResponse.getHits(1).getScore() > 1.0);
    assertTrue(searchResponse.getHits(2).getScore() <= 1.0);
    assertEquals(2, searchResponse.getDiagnostics().getVectorDiagnosticsCount());
  }

  @Test
  public void testHybridVectorSearch() {
    List<Float> queryVector = List.of(0.05f, 0.5f, 0.75f);
    String field = "vector_cosine";
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .addRetrieveFields("filter")
                    .setStartHit(0)
                    .setTopHits(30)
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder()
                                    .setField("filter")
                                    .setTextValue("term1")
                                    .build())
                            .build())
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(30)
                            .setK(15)
                            .setBoost(50)
                            .build())
                    .build());
    assertEquals(30, searchResponse.getHitsCount());
    // hits with vector component
    float[] queryVectorArray = floatListToArray(queryVector);
    Set<String> terms = new HashSet<>();
    for (int i = 0; i < 15; ++i) {
      Hit hit = searchResponse.getHits(i);
      String term = hit.getFieldsOrThrow("filter").getFieldValue(0).getTextValue();
      terms.add(term);
      float similarity =
          VectorSimilarityFunction.COSINE.compare(
                  queryVectorArray,
                  floatListToArray(
                      hit.getFieldsOrThrow(field).getFieldValue(0).getVectorValue().getValueList()))
              * 50.0f;
      if ("term1".equals(term)) {
        // vector + query score
        assertTrue(hit.getScore() > similarity + 0.0001);
      } else {
        // only vector score
        assertEquals(similarity, hit.getScore(), 0.0001);
      }
    }
    assertTrue(terms.size() > 1);
    assertTrue(terms.contains("term1"));

    // just query component
    for (int i = 15; i < 30; ++i) {
      Hit hit = searchResponse.getHits(i);
      assertTrue(hit.getScore() < 10.0);
      String term = hit.getFieldsOrThrow("filter").getFieldValue(0).getTextValue();
      assertEquals("term1", term);
    }
  }

  private void singleVectorQueryAndVerify(
      String field,
      List<Float> queryVector,
      VectorSimilarityFunction similarityFunction,
      float boost) {
    singleVectorQueryAndVerify(field, queryVector, similarityFunction, boost, 0.0001);
  }

  private void singleNormCosineQueryAndVerify(List<Float> queryVector, float boost) {
    singleNormCosineQueryAndVerify(queryVector, boost, 0.0001);
  }

  private void singleVectorQueryAndVerify(
      String field,
      List<Float> queryVector,
      VectorSimilarityFunction similarityFunction,
      float boost,
      double delta) {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .setBoost(boost)
                            .build())
                    .build());
    assertEquals(5, searchResponse.getHitsCount());
    assertEquals(1, searchResponse.getDiagnostics().getVectorDiagnosticsCount());
    VectorDiagnostics vectorDiagnostics = searchResponse.getDiagnostics().getVectorDiagnostics(0);
    assertTrue(vectorDiagnostics.getSearchTimeMs() > 0.0);
    assertTrue(vectorDiagnostics.getTotalHits().getValue() > 0);
    verifyHitsSimilarity(field, queryVector, searchResponse, similarityFunction, boost, delta);
  }

  private void singleNormCosineQueryAndVerify(List<Float> queryVector, float boost, double delta) {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields("vector_cosine")
                    .addRetrieveFields("vector_cosine.normalized")
                    .addRetrieveFields("vector_cosine.normalized._magnitude")
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("vector_cosine.normalized")
                            .addAllQueryVector(queryVector)
                            .setNumCandidates(10)
                            .setK(5)
                            .setBoost(boost)
                            .build())
                    .build());
    assertEquals(5, searchResponse.getHitsCount());
    assertEquals(1, searchResponse.getDiagnostics().getVectorDiagnosticsCount());
    VectorDiagnostics vectorDiagnostics = searchResponse.getDiagnostics().getVectorDiagnostics(0);
    assertTrue(vectorDiagnostics.getSearchTimeMs() > 0.0);
    assertTrue(vectorDiagnostics.getTotalHits().getValue() > 0);
    verifyHitsNormCosine(queryVector, searchResponse, boost, delta);
  }

  private void verifyHitsSimilarity(
      String field,
      List<Float> queryVector,
      SearchResponse response,
      VectorSimilarityFunction similarityFunction,
      float boost,
      double delta) {
    float[] queryVectorArray = floatListToArray(queryVector);
    for (Hit hit : response.getHitsList()) {
      float similarity =
          similarityFunction.compare(
                  queryVectorArray,
                  floatListToArray(
                      hit.getFieldsOrThrow(field).getFieldValue(0).getVectorValue().getValueList()))
              * boost;
      assertEquals(similarity, hit.getScore(), delta);
    }
  }

  private void verifyHitsNormCosine(
      List<Float> queryVector, SearchResponse response, float boost, double delta) {
    float[] queryVectorArray = floatListToArray(queryVector);
    VectorUtil.l2normalize(queryVectorArray);
    for (Hit hit : response.getHitsList()) {
      float[] vector =
          floatListToArray(
              hit.getFieldsOrThrow("vector_cosine.normalized")
                  .getFieldValue(0)
                  .getVectorValue()
                  .getValueList());
      assertTrue(VectorUtil.isUnitVector(vector));
      float similarity =
          VectorSimilarityFunction.DOT_PRODUCT.compare(queryVectorArray, vector) * boost;
      assertEquals(similarity, hit.getScore(), delta);

      float[] nonNormalizedVector =
          floatListToArray(
              hit.getFieldsOrThrow("vector_cosine")
                  .getFieldValue(0)
                  .getVectorValue()
                  .getValueList());
      float magnitude =
          hit.getFieldsOrThrow("vector_cosine.normalized._magnitude")
              .getFieldValue(0)
              .getFloatValue();
      for (int i = 0; i < vector.length; i++) {
        vector[i] *= magnitude;
      }
      assertArrayEquals(nonNormalizedVector, vector, (float) delta);
    }
  }

  private float[] floatListToArray(List<Float> list) {
    float[] array = new float[list.size()];
    for (int i = 0; i < list.size(); ++i) {
      array[i] = list.get(i);
    }
    return array;
  }

  private void singleByteVectorQueryAndVerify(
      String field, byte[] queryVector, VectorSimilarityFunction similarityFunction, float boost) {
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                    .addRetrieveFields(field)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField(field)
                            .setQueryByteVector(ByteString.copyFrom(queryVector))
                            .setNumCandidates(10)
                            .setK(5)
                            .setBoost(boost)
                            .build())
                    .build());
    assertEquals(5, searchResponse.getHitsCount());
    assertEquals(1, searchResponse.getDiagnostics().getVectorDiagnosticsCount());
    VectorDiagnostics vectorDiagnostics = searchResponse.getDiagnostics().getVectorDiagnostics(0);
    assertTrue(vectorDiagnostics.getSearchTimeMs() > 0.0);
    assertTrue(vectorDiagnostics.getTotalHits().getValue() > 0);
    verifyByteHitsSimilarity(field, queryVector, searchResponse, similarityFunction, boost);
  }

  private void verifyByteHitsSimilarity(
      String field,
      byte[] queryVector,
      SearchResponse response,
      VectorSimilarityFunction similarityFunction,
      float boost) {
    for (Hit hit : response.getHitsList()) {
      float similarity =
          similarityFunction.compare(
                  queryVector,
                  hit.getFieldsOrThrow(field)
                      .getFieldValue(0)
                      .getVectorValue()
                      .getBytesValue()
                      .toByteArray())
              * boost;
      assertEquals(similarity, hit.getScore(), 0.0001);
    }
  }

  @Test
  public void testVectorFormat_none() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .build();
    VectorFieldDef<?> vectorFieldDef =
        VectorFieldDef.createField(
            "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertNotNull(format);
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=16, beamWidth=100, flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer()))",
        format.toString());
  }

  @Test
  public void testVectorFormat_set_m() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder().setType("hnsw").setHnswM(5).build())
            .build();
    VectorFieldDef<?> vectorFieldDef =
        VectorFieldDef.createField(
            "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertNotNull(format);
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=5, beamWidth=100, flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer()))",
        format.toString());
  }

  @Test
  public void testVectorFormat_set_ef_construction() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder()
                    .setType("hnsw")
                    .setHnswEfConstruction(50)
                    .build())
            .build();
    VectorFieldDef<?> vectorFieldDef =
        VectorFieldDef.createField(
            "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertNotNull(format);
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=16, beamWidth=50, flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer()))",
        format.toString());
  }

  @Test
  public void testVectorFormat_invalid_type() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(VectorIndexingOptions.newBuilder().setType("invalid").build())
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage()
              .startsWith("Unexpected vector search type \"invalid\", expected one of: ["));
    }
  }

  @Test
  public void testVectorFormat_invalid_similarity() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("invalid")
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage()
              .startsWith("Unexpected vector similarity \"invalid\", expected one of: ["));
    }
  }

  @Test
  public void testMaxDocValueDimensions() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setVectorDimensions(4097)
            .setStoreDocValues(true)
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector dimension must be <= 4096", e.getMessage());
    }
  }

  @Test
  public void testMaxSearchDimensions() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setVectorDimensions(4097)
            .setSearch(true)
            .setVectorSimilarity("cosine")
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector dimension must be <= 4096", e.getMessage());
    }
  }

  @Test
  public void testVectorFormat_invalidNormCosineByte() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("normalized_cosine")
            .setVectorElementType(VectorElementType.VECTOR_ELEMENT_BYTE)
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Normalized cosine similarity is not supported for byte vectors", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_nan() throws IOException {
    VectorFieldDef.FloatVectorFieldDef vectorFieldDef = getCosineField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, Float.NaN, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("non-finite value at vector[1]=NaN", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_inf() throws IOException {
    VectorFieldDef.FloatVectorFieldDef vectorFieldDef = getCosineField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, Float.POSITIVE_INFINITY, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("non-finite value at vector[1]=Infinity", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_zero_magnitude() throws IOException {
    VectorFieldDef.FloatVectorFieldDef vectorFieldDef = getL2Field();
    vectorFieldDef.validateVectorForSearch(new float[] {0.0f, 0.0f, 0.0f});

    vectorFieldDef = getCosineField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {0.0f, 0.0f, 0.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector magnitude cannot be 0 when using cosine similarity", e.getMessage());
    }
  }

  @Test
  public void testValidateByteVector_zero_magnitude() throws IOException {
    VectorFieldDef.ByteVectorFieldDef vectorFieldDef = getL2ByteField();
    vectorFieldDef.validateVectorForSearch(new byte[] {0, 0, 0});

    vectorFieldDef = getCosineByteField();
    try {
      vectorFieldDef.validateVectorForSearch(new byte[] {0, 0, 0});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector magnitude cannot be 0 when using cosine similarity", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_not_normalized() throws IOException {
    VectorFieldDef.FloatVectorFieldDef vectorFieldDef = getDotField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, 1.0f, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector must be normalized when using dot product similarity", e.getMessage());
    }
  }

  private VectorFieldDef.FloatVectorFieldDef getCosineField() throws IOException {
    return (VectorFieldDef.FloatVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("vector_cosine");
  }

  private VectorFieldDef.FloatVectorFieldDef getNormCosineField() throws IOException {
    return (VectorFieldDef.FloatVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("vector_cosine.normalized");
  }

  private VectorFieldDef.FloatVectorFieldDef getL2Field() throws IOException {
    return (VectorFieldDef.FloatVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("vector_l2_norm");
  }

  private VectorFieldDef.FloatVectorFieldDef getDotField() throws IOException {
    return (VectorFieldDef.FloatVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("vector_dot");
  }

  private VectorFieldDef.ByteVectorFieldDef getCosineByteField() throws IOException {
    return (VectorFieldDef.ByteVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("byte_vector_cosine");
  }

  private VectorFieldDef.ByteVectorFieldDef getL2ByteField() throws IOException {
    return (VectorFieldDef.ByteVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("byte_vector_l2_norm");
  }

  private VectorFieldDef.FloatVectorFieldDef getQuantizedField() throws IOException {
    return (VectorFieldDef.FloatVectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndexOrThrow(VECTOR_SEARCH_INDEX_NAME)
            .getFieldOrThrow("quantized_vector_7");
  }

  @Test
  public void testFieldNotExist() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("non_existent_field")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(10)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("field \"non_existent_field\" is unknown"));
    }
  }

  @Test
  public void testFieldNotVector() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("filter")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(10)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field does not support vector search: filter"));
    }
  }

  @Test
  public void testFieldNotSearchable() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("vector_not_search")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(10)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Vector field is not searchable: vector_not_search"));
    }
  }

  @Test
  public void testInvalidVectorSize() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("vector_l2_norm")
                          .addAllQueryVector(List.of(0.25f, 0.5f))
                          .setNumCandidates(10)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Invalid query vector size, expected: 3, found: 2"));
    }
  }

  @Test
  public void testInvalidK() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("vector_l2_norm")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(10)
                          .setK(0)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Vector search k must be >= 1"));
    }
  }

  @Test
  public void testNumCandidatesLessThanK() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("vector_l2_norm")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(4)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Vector search numCandidates must be >= k"));
    }
  }

  @Test
  public void testNumCandidatesMaxLimit() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(VECTOR_SEARCH_INDEX_NAME)
                  .setStartHit(0)
                  .setTopHits(10)
                  .addKnn(
                      KnnQuery.newBuilder()
                          .setField("vector_l2_norm")
                          .addAllQueryVector(List.of(0.25f, 0.5f, 0.75f))
                          .setNumCandidates(10001)
                          .setK(5)
                          .build())
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Vector search numCandidates > 10000"));
    }
  }

  @Test
  public void testIndexByteOutOfRange() {
    List<AddDocumentRequest> documents = new ArrayList<>();
    documents.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(VECTOR_SEARCH_INDEX_NAME)
            .putFields(
                "byte_vector_l2_norm",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("[128, 5, 100]").build())
            .build());
    try {
      addDocuments(documents.stream());
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Byte value out of range: 128 at index: 0"));
    }
  }

  @Test
  public void testIndexFloatAsByte() {
    List<AddDocumentRequest> documents = new ArrayList<>();
    documents.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(VECTOR_SEARCH_INDEX_NAME)
            .putFields(
                "byte_vector_l2_norm",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("[0, 5.0, 100.1]")
                    .build())
            .build());
    try {
      addDocuments(documents.stream());
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Byte value is not an integer: 100.1 at index: 2"));
    }
  }

  @Test
  public void testGetVectorFormat() throws IOException {
    VectorFieldDef<?> vectorFieldDef = getCosineField();
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=16, beamWidth=100, flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer()))",
        format.toString());
  }

  @Test
  public void testGetQuantizedVectorFormat() throws IOException {
    VectorFieldDef<?> vectorFieldDef = getQuantizedField();
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertEquals(
        "Lucene99HnswScalarQuantizedVectorsFormat(name=Lucene99HnswScalarQuantizedVectorsFormat, maxConn=16, beamWidth=100, flatVectorFormat=Lucene99ScalarQuantizedVectorsFormat(name=Lucene99ScalarQuantizedVectorsFormat, confidenceInterval=null, bits=7, compress=false, flatVectorScorer=ScalarQuantizedVectorScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=DefaultFlatVectorScorer())))",
        format.toString());
  }

  @Test
  public void testInvalidConfidenceInterval() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder()
                    .setType("hnsw_scalar_quantized")
                    .setQuantizedConfidenceInterval(0.5f)
                    .build())
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "confidenceInterval must be between 0.9 and 1.0 or 0; confidenceInterval=0.5",
          e.getMessage());
    }
  }

  @Test
  public void testInvalidBits() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder()
                    .setType("hnsw_scalar_quantized")
                    .setQuantizedBits(9)
                    .build())
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("bits must be one of: 4, 7; bits=9", e.getMessage());
    }
  }

  @Test
  public void testInvalidQuantizedByteVector() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorElementType(VectorElementType.VECTOR_ELEMENT_BYTE)
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder().setType("hnsw_scalar_quantized").build())
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "HNSW scalar quantized search type is only supported for float vectors", e.getMessage());
    }
  }

  @Test
  public void testInvalid4BitsOddDimensions() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setSearch(true)
            .setVectorDimensions(3)
            .setVectorSimilarity("l2_norm")
            .setVectorIndexingOptions(
                VectorIndexingOptions.newBuilder()
                    .setType("hnsw_scalar_quantized")
                    .setQuantizedBits(4)
                    .build())
            .build();
    try {
      VectorFieldDef.createField(
          "vector", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "HNSW scalar quantized search type with 4 bits requires vector dimensions to be a multiple of 2",
          e.getMessage());
    }
  }

  @Test
  public void testNestedFloatVectorSearch() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("id")
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("nested_object.float_vector")
                            .addAllQueryVector(List.of(0.6f, 0.5f, 0.75f))
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(2, response.getHitsCount());
    assertEquals("1", response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(
            new float[] {0.6f, 0.5f, 0.75f}, new float[] {0.7f, 0.8f, 0.9f}),
        response.getHits(0).getScore(),
        0.0001);
    assertEquals("0", response.getHits(1).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(
            new float[] {0.6f, 0.5f, 0.75f}, new float[] {0.2f, 0.4f, 0.3f}),
        response.getHits(1).getScore(),
        0.0001);

    assertEquals(1, response.getDiagnostics().getVectorDiagnosticsCount());
    VectorDiagnostics vectorDiagnostics = response.getDiagnostics().getVectorDiagnostics(0);
    assertTrue(vectorDiagnostics.getSearchTimeMs() > 0.0);
    assertEquals(4, vectorDiagnostics.getTotalHits().getValue());
  }

  @Test
  public void testNestedFloatVectorSearchWithFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("id")
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("nested_object.float_vector")
                            .addAllQueryVector(List.of(0.6f, 0.5f, 0.75f))
                            .setNumCandidates(10)
                            .setK(5)
                            .setFilter(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("filter_field")
                                            .setIntValue(1)
                                            .build())
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals("0", response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(
            new float[] {0.6f, 0.5f, 0.75f}, new float[] {0.2f, 0.4f, 0.3f}),
        response.getHits(0).getScore(),
        0.0001);
  }

  @Test
  public void testNestedByteVectorSearch() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("id")
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("nested_object.byte_vector")
                            .setQueryByteVector(ByteString.copyFrom(new byte[] {1, 2, 3}))
                            .setNumCandidates(10)
                            .setK(5)
                            .build())
                    .build());
    assertEquals(2, response.getHitsCount());
    assertEquals("1", response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(new byte[] {1, 2, 3}, new byte[] {8, 10, 1}),
        response.getHits(0).getScore(),
        0.0001);
    assertEquals("0", response.getHits(1).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(new byte[] {1, 2, 3}, new byte[] {-50, -5, 0}),
        response.getHits(1).getScore(),
        0.0001);

    assertEquals(1, response.getDiagnostics().getVectorDiagnosticsCount());
    VectorDiagnostics vectorDiagnostics = response.getDiagnostics().getVectorDiagnostics(0);
    assertTrue(vectorDiagnostics.getSearchTimeMs() > 0.0);
    assertEquals(4, vectorDiagnostics.getTotalHits().getValue());
  }

  @Test
  public void testNestedByteVectorSearchWithFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(NESTED_VECTOR_SEARCH_INDEX_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("id")
                    .addKnn(
                        KnnQuery.newBuilder()
                            .setField("nested_object.byte_vector")
                            .setQueryByteVector(ByteString.copyFrom(new byte[] {1, 2, 3}))
                            .setNumCandidates(10)
                            .setK(5)
                            .setFilter(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("filter_field")
                                            .setIntValue(1)
                                            .build())
                                    .build())
                            .build())
                    .build());
    assertEquals(1, response.getHitsCount());
    assertEquals("0", response.getHits(0).getFieldsOrThrow("id").getFieldValue(0).getTextValue());
    assertEquals(
        VectorSimilarityFunction.COSINE.compare(new byte[] {1, 2, 3}, new byte[] {-50, -5, 0}),
        response.getHits(0).getScore(),
        0.0001);
  }
}
