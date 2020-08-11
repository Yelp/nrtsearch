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
import com.yelp.nrtsearch.server.grpc.FacetHierarchyPath;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LabelAndValue;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;

public class TextFieldFacetsTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private List<AddDocumentRequest> buildDocumentsWithPath(String indexName) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "hierarchy_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("home mike work")
                    .addFaceHierarchyPaths(
                        FacetHierarchyPath.newBuilder()
                            .addValue("home")
                            .addValue("mike")
                            .addValue("work")
                            .build())
                    .addValue("home john work")
                    .addFaceHierarchyPaths(
                        FacetHierarchyPath.newBuilder()
                            .addValue("home")
                            .addValue("john")
                            .addValue("work")
                            .build())
                    .addValue("home john personal")
                    .addFaceHierarchyPaths(
                        FacetHierarchyPath.newBuilder()
                            .addValue("home")
                            .addValue("john")
                            .addValue("personal")
                            .build())
                    .build())
            .build());

    return documentRequests;
  }

  private List<AddDocumentRequest> buildDocuments(String indexName) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "sorted_doc_values_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Doe")
                    .build())
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .putFields(
                "flat_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Doe")
                    .build())
            .putFields(
                "flat_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .putFields(
                "hierarchy_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Doe")
                    .build())
            .putFields(
                "hierarchy_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "sorted_doc_values_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Smith")
                    .build())
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .putFields(
                "flat_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Smith")
                    .build())
            .putFields(
                "flat_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .putFields(
                "hierarchy_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("John")
                    .addValue("Smith")
                    .build())
            .putFields(
                "hierarchy_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "flat_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "hierarchy_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    return documentRequests;
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/text_field_facets.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name);
    addDocuments(documents.stream());
    addDocuments(buildDocumentsWithPath(name).stream()); // for hierarchy path faceting
  }

  private SearchResponse getSearchResponse(String dimension, String... paths) {
    return getSearchResponse(dimension, false, paths);
  }

  private SearchResponse getSearchResponse(
      String dimension, boolean useOrdsCache, String... paths) {
    Facet.Builder facetBuilder =
        Facet.newBuilder().setDim(dimension).setTopN(10).setUseOrdsCache(useOrdsCache);
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
  public void testSortedDocValuesFacet() {
    SearchResponse response = getSearchResponse("sorted_doc_values_facet_field");
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(2.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Doe").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Smith").setValue(1.0).build());
    assertFacetResult(
        facetResults.get(0), "sorted_doc_values_facet_field", 4, 3L, expectedLabelAndValues);
  }

  @Test
  public void testSortedDocValuesSingleValued() {
    SearchResponse response = getSearchResponse("sorted_doc_values_facet_field_single_valued");
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(3.0).build());
    assertFacetResult(
        facetResults.get(0),
        "sorted_doc_values_facet_field_single_valued",
        3,
        1L,
        expectedLabelAndValues);
  }

  @Test
  public void testHierarchyMultivaluedNoPath() {
    SearchResponse response = getSearchResponse("hierarchy_facet_field");
    assertFacetResultMatch(response);
  }

  public void assertFacetResultMatch(SearchResponse response) {
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(2.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Doe").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Smith").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("home").setValue(1.0).build());
    // NOTE: total number of buckets/value returned by FastTaxonomyFacetCounts is -1 on multivalued
    // fields.
    assertFacetResult(facetResults.get(0), "hierarchy_facet_field", -1, 4L, expectedLabelAndValues);
  }

  @Test
  public void testHierarchyMultivaluedNoPathUseOrdinalsCache() {
    SearchResponse response = getSearchResponse("hierarchy_facet_field", true);
    assertFacetResultMatch(response);
  }

  @Test
  public void testHierarchyWithPath() {
    SearchResponse response = getSearchResponse("hierarchy_facet_field", "home");
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("mike").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("john").setValue(1.0).build());
    // NOTE: total number of buckets/value returned by FastTaxonomyFacetCounts is -1 on multivalued
    // fields.
    assertFacetResult(
        facetResults.get(0), "hierarchy_facet_field", -1, 2L, expectedLabelAndValues, "home");

    response = getSearchResponse("hierarchy_facet_field", "home", "john");
    facetResults = response.getFacetResultList();
    expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("work").setValue(1.0).build());
    expectedLabelAndValues.add(
        LabelAndValue.newBuilder().setLabel("personal").setValue(1.0).build());
    // NOTE: total number of buckets/value returned by FastTaxonomyFacetCounts is -1 on multivalued
    // fields.
    assertFacetResult(
        facetResults.get(0),
        "hierarchy_facet_field",
        -1,
        2L,
        expectedLabelAndValues,
        "home",
        "john");
  }

  @Test
  public void testHierarchySingleValued() {
    SearchResponse response = getSearchResponse("hierarchy_facet_field_single_valued");
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(3.0).build());
    assertFacetResult(
        facetResults.get(0), "hierarchy_facet_field_single_valued", 3, 1L, expectedLabelAndValues);
  }

  @Test
  public void testFlatMultivalued() {
    SearchResponse response = getSearchResponse("flat_facet_field");
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(2.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Doe").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("Smith").setValue(1.0).build());
    // NOTE: total number of buckets/value returned by FastTaxonomyFacetCounts is -1 on multivalued
    // fields.
    assertFacetResult(facetResults.get(0), "flat_facet_field", -1, 3L, expectedLabelAndValues);
  }

  @Test
  public void testFlatSingleValued() {
    SearchResponse response = getSearchResponse("flat_facet_field_single_valued");
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("John").setValue(3.0).build());
    assertFacetResult(
        facetResults.get(0), "flat_facet_field_single_valued", 3, 1L, expectedLabelAndValues);
  }

  private void assertFacetResult(
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
