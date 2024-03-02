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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.Floats;
import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FieldType;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics.VectorDiagnostics;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue.Vector;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.VectorIndexingOptions;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class VectorFieldDefTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static final String VECTOR_SEARCH_INDEX_NAME = "vector_search_index";
  private static final String FIELD_NAME = "vector_field";
  private static final String FIELD_TYPE = "VECTOR";
  private static final List<String> VECTOR_FIELD_VALUES =
      Arrays.asList("[1.0, 2.5, 1000.1000]", "[0.1, -2.0, 5.6]");

  @Override
  protected List<String> getIndices() {
    return List.of(DEFAULT_TEST_INDEX, VECTOR_SEARCH_INDEX_NAME);
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
    IndexWriter writer = getGlobalState().getIndex(VECTOR_SEARCH_INDEX_NAME).getShard(0).writer;
    // don't want any merges for these tests to verify parallel search
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    getGlobalState()
        .getIndexStateManager(DEFAULT_TEST_INDEX)
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
                    "filter", MultiValuedField.newBuilder().addValue("term" + j % 10).build())
                .build());
      }

      addDocuments(docs.stream());
      writer.flush();
    }
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

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    if (DEFAULT_TEST_INDEX.equals(name)) {
      return getFieldsFromResourceFile("/field/registerFieldsVector.json");
    } else if (VECTOR_SEARCH_INDEX_NAME.equals(name)) {
      return getFieldsFromResourceFile("/field/registerFieldsVectorSearch.json");
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
    } else {
      throw new IllegalArgumentException("Unknown index name: " + name);
    }
  }

  public FieldDef getFieldDef(String testIndex, String fieldName) throws IOException {
    return getGrpcServer().getGlobalState().getIndex(testIndex).getField(fieldName);
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
  public void testVectorSearch_dot() {
    singleVectorQueryAndVerify(
        "vector_dot",
        normalizeVector(List.of(0.25f, 0.5f, 0.75f)),
        VectorSimilarityFunction.DOT_PRODUCT,
        1.0f);
  }

  @Test
  public void testVectorSearch_boost() {
    singleVectorQueryAndVerify(
        "vector_l2_norm", List.of(0.25f, 0.5f, 0.75f), VectorSimilarityFunction.EUCLIDEAN, 2.0f);
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
    verifyHitsSimilarity(field, queryVector, searchResponse, VectorSimilarityFunction.COSINE, 1.0f);
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
    verifyHitsSimilarity(field, queryVector, searchResponse, similarityFunction, boost);
  }

  private void verifyHitsSimilarity(
      String field,
      List<Float> queryVector,
      SearchResponse response,
      VectorSimilarityFunction similarityFunction,
      float boost) {
    float[] queryVectorArray = floatListToArray(queryVector);
    for (Hit hit : response.getHitsList()) {
      float similarity =
          similarityFunction.compare(
                  queryVectorArray,
                  floatListToArray(
                      hit.getFieldsOrThrow(field).getFieldValue(0).getVectorValue().getValueList()))
              * boost;
      assertEquals(similarity, hit.getScore(), 0.0001);
    }
  }

  private float[] floatListToArray(List<Float> list) {
    float[] array = new float[list.size()];
    for (int i = 0; i < list.size(); ++i) {
      array[i] = list.get(i);
    }
    return array;
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
    VectorFieldDef vectorFieldDef = new VectorFieldDef("vector", field);
    assertNull(vectorFieldDef.getVectorsFormat());
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
    VectorFieldDef vectorFieldDef = new VectorFieldDef("vector", field);
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertNotNull(format);
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=5, beamWidth=100)",
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
    VectorFieldDef vectorFieldDef = new VectorFieldDef("vector", field);
    KnnVectorsFormat format = vectorFieldDef.getVectorsFormat();
    assertNotNull(format);
    assertEquals(
        "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn=16, beamWidth=50)",
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
      new VectorFieldDef("vector", field);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unexpected vector format type \"invalid\", expected: hnsw", e.getMessage());
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
      new VectorFieldDef("vector", field);
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
            .setVectorDimensions(2049)
            .setStoreDocValues(true)
            .build();
    try {
      new VectorFieldDef("vector", field);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector dimension must be <= 2048 for doc values", e.getMessage());
    }
  }

  @Test
  public void testMaxSearchDimensions() {
    Field field =
        Field.newBuilder()
            .setName("vector")
            .setType(FieldType.VECTOR)
            .setVectorDimensions(1025)
            .setSearch(true)
            .setVectorSimilarity("cosine")
            .build();
    try {
      new VectorFieldDef("vector", field);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector dimension must be <= 1024 for search", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_nan() throws IOException {
    VectorFieldDef vectorFieldDef = getCosineField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, Float.NaN, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector component cannot be NaN", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_inf() throws IOException {
    VectorFieldDef vectorFieldDef = getCosineField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, Float.POSITIVE_INFINITY, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector component cannot be Infinite", e.getMessage());
    }
  }

  @Test
  public void testValidateVector_zero_magnitude() throws IOException {
    VectorFieldDef vectorFieldDef = getL2Field();
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
  public void testValidateVector_not_normalized() throws IOException {
    VectorFieldDef vectorFieldDef = getDotField();
    try {
      vectorFieldDef.validateVectorForSearch(new float[] {1.0f, 1.0f, 1.0f});
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Vector must be normalized when using dot product similarity", e.getMessage());
    }
  }

  private VectorFieldDef getCosineField() throws IOException {
    return (VectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndex(VECTOR_SEARCH_INDEX_NAME)
            .getField("vector_cosine");
  }

  private VectorFieldDef getL2Field() throws IOException {
    return (VectorFieldDef)
        getGrpcServer()
            .getGlobalState()
            .getIndex(VECTOR_SEARCH_INDEX_NAME)
            .getField("vector_l2_norm");
  }

  private VectorFieldDef getDotField() throws IOException {
    return (VectorFieldDef)
        getGrpcServer().getGlobalState().getIndex(VECTOR_SEARCH_INDEX_NAME).getField("vector_dot");
  }
}
