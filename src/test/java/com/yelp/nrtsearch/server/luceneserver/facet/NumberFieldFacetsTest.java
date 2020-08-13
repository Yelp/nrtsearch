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
package com.yelp.nrtsearch.server.luceneserver.facet;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LabelAndValue;
import com.yelp.nrtsearch.server.grpc.NumericRangeType;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class NumberFieldFacetsTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final List<String> fields =
      Arrays.asList(
          new String[] {
            "int_number_facet_field",
            "float_number_facet_field",
            "long_number_facet_field",
            "double_number_facet_field"
          });
  private static final List<String> numericValues =
      Arrays.asList(new String[] {"1", "10", "20", "30"});

  private Map<String, AddDocumentRequest.MultiValuedField> getFieldsMapForOneDocument(
      String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap = new HashMap<>();
    for (String field : fields) {
      fieldsMap.put(
          field, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    }
    return fieldsMap;
  }

  private List<AddDocumentRequest> buildDocuments(String indexName) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : numericValues) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    return documentRequests;
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/number_field_facets.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name);
    addDocuments(documents.stream());
  }

  private SearchResponse getSearchResponse(String dimension, String... paths) {
    return getSearchResponse(dimension, false, Collections.emptyList(), paths);
  }

  private SearchResponse getSearchResponse(
      String dimension,
      boolean useOrdsCache,
      List<NumericRangeType> numericRangeTypes,
      String... paths) {
    Facet.Builder facetBuilder =
        Facet.newBuilder()
            .setDim(dimension)
            .setTopN(10)
            .setUseOrdsCache(useOrdsCache)
            .addAllNumericRange(numericRangeTypes);
    facetBuilder.addAllPaths(Arrays.asList(paths));
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setTopHits(10)
                .addFacets(facetBuilder.build())
                .build());
  }

  @Test
  public void testIntNumberNumericRange() {
    assertNumericRangeFacet("int_number_facet_field");
  }

  @Test
  public void testLongNumberNumericRange() {
    assertNumericRangeFacet("long_number_facet_field");
  }

  @Test(expected = StatusRuntimeException.class)
  public void testFloatNumberNumericRange() {
    assertNumericRangeFacet("float_number_facet_field");
  }

  @Test
  public void testDoubleNumberNumericRange() {
    assertNumericRangeFacet("double_number_facet_field");
  }

  private void assertNumericRangeFacet(String fieldName) {
    List<NumericRangeType> numericRangeTypes = new ArrayList<>();
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("1-10")
            .setMin(1L)
            .setMinInclusive(true)
            .setMax(10L)
            .setMaxInclusive(true)
            .build());
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("11-20")
            .setMin(11L)
            .setMinInclusive(true)
            .setMax(20L)
            .setMaxInclusive(true)
            .build());
    SearchResponse response = getSearchResponse(fieldName, false, numericRangeTypes);
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("1-10").setValue(2.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("11-20").setValue(1.0).build());
    assertFacetResult(facetResults.get(0), fieldName, 3, 2L, expectedLabelAndValues);
  }

  static void assertFacetResult(
      FacetResult testFacetResult,
      String dim, // dimension against which to bucket/group also the fieldName
      double
          value, // total values across all bucket i.e. if we add up lavelAndValue.value across all
      // labels not just topN
      long childCount, // total number of distinct buckets/terms
      List<LabelAndValue> expectedLableValues,
      String... paths) {
    assertEquals(dim, testFacetResult.getDim());
    assertEquals(value, testFacetResult.getValue(), 0.001);
    assertEquals(childCount, testFacetResult.getChildCount());
    assertEquals(expectedLableValues.size(), testFacetResult.getLabelValuesList().size());
    for (int i = 0; i < expectedLableValues.size(); i++) {
      assertEquals(
          expectedLableValues.get(i).getLabel(), testFacetResult.getLabelValues(i).getLabel());
      assertEquals(
          expectedLableValues.get(i).getValue(),
          testFacetResult.getLabelValues(i).getValue(),
          0.001);
    }
    for (int i = 0; i < paths.length; i++) {
      assertEquals(paths[i], testFacetResult.getPath(i));
    }
  }
}
