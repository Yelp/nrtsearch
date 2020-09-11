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

import static com.yelp.nrtsearch.server.luceneserver.facet.NumberFieldFacetsTest.assertFacetResult;
import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LabelAndValue;
import com.yelp.nrtsearch.server.grpc.NumericRangeType;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class VirtualFieldFacetsTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private List<AddDocumentRequest> buildDocuments(String indexName) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "int_number_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("10").build())
            .putFields(
                "text_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    documentRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(indexName)
            .putFields(
                "int_number_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("20").build())
            .putFields(
                "text_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("John").build())
            .build());
    return documentRequests;
  }

  private SearchResponse getSearchResponse(
      String dimension,
      boolean useOrdsCache,
      List<NumericRangeType> numericRangeTypes,
      String scriptSource,
      Map<String, Script.ParamValue> params,
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
                .addVirtualFields(
                    VirtualField.newBuilder()
                        .setScript(
                            Script.newBuilder()
                                .setLang("js")
                                .setSource(scriptSource)
                                .putAllParams(params)
                                .build())
                        .setName(dimension)
                        .build())
                .addFacets(facetBuilder.build())
                .build());
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/facet/virtual_field_facets.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name);
    addDocuments(documents.stream());
  }

  @Test
  public void testVirtualField() {
    assertEquals(0, 0);
    List<NumericRangeType> numericRangeTypes = new ArrayList<>();
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("1-50")
            .setMin(1L)
            .setMinInclusive(true)
            .setMax(50L)
            .setMaxInclusive(true)
            .build());
    numericRangeTypes.add(
        NumericRangeType.newBuilder()
            .setLabel("51-100")
            .setMin(51L)
            .setMinInclusive(true)
            .setMax(100L)
            .setMaxInclusive(true)
            .build());
    String dim = "virtual_field_js_script";
    SearchResponse response =
        getSearchResponse(
            dim, false, numericRangeTypes, "int_number_facet_field*5.0", Collections.emptyMap());
    assertEquals(1, response.getFacetResultCount());
    List<FacetResult> facetResults = response.getFacetResultList();
    List<LabelAndValue> expectedLabelAndValues = new ArrayList<>();
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("1-50").setValue(1.0).build());
    expectedLabelAndValues.add(LabelAndValue.newBuilder().setLabel("51-100").setValue(1.0).build());
    assertFacetResult(facetResults.get(0), dim, 2, 2L, expectedLabelAndValues);
  }
}
