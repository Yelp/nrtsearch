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
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.junit.ClassRule;
import org.junit.Test;

public class FilteredSSDVFacetCountsTest extends ServerTestCase {
  private static final String MULTI_SEGMENT_INDEX = "test_index_multi";
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    if (MULTI_SEGMENT_INDEX.equals(name)) {
      return getFieldsFromResourceFile("/facet/filtered_field_facets_multi.json");
    } else {
      return getFieldsFromResourceFile("/facet/filtered_field_facets.json");
    }
  }

  @Override
  public List<String> getIndices() {
    return Arrays.asList(DEFAULT_TEST_INDEX, MULTI_SEGMENT_INDEX);
  }

  @Override
  public void initIndex(String name) throws Exception {
    IndexWriter writer = getGlobalState().getIndex(name).getShard(0).writer;
    // don't want any merges for these tests
    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

    AddDocumentRequest doc1 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "sorted_doc_values_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("M1")
                    .addValue("M2")
                    .build())
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("S1").build())
            .build();
    AddDocumentRequest doc2 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "sorted_doc_values_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("M2").build())
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("S1").build())
            .build();
    AddDocumentRequest doc3 =
        AddDocumentRequest.newBuilder()
            .setIndexName(name)
            .putFields(
                "sorted_doc_values_facet_field",
                AddDocumentRequest.MultiValuedField.newBuilder()
                    .addValue("M1")
                    .addValue("M2")
                    .addValue("M3")
                    .build())
            .putFields(
                "sorted_doc_values_facet_field_single_valued",
                AddDocumentRequest.MultiValuedField.newBuilder().addValue("S2").build())
            .build();

    if (MULTI_SEGMENT_INDEX.equals(name)) {
      addDocuments(Stream.of(doc1));
      writer.commit();
      addDocuments(Stream.of(doc2));
      writer.commit();
      addDocuments(Stream.of(doc3));
      writer.commit();
    } else {
      List<AddDocumentRequest> documents = new ArrayList<>();
      documents.add(doc1);
      documents.add(doc2);
      documents.add(doc3);
      addDocuments(documents.stream());
    }
  }

  @Test
  public void testFiltered() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field")
            .addLabels("M1")
            .addLabels("M2")
            .setTopN(3)
            .build();
    queryAndVerify(
        facet,
        2,
        LabelAndValue.newBuilder().setLabel("M2").setValue(3).build(),
        LabelAndValue.newBuilder().setLabel("M1").setValue(2).build());
  }

  @Test
  public void testFilteredWithNotPresent() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field")
            .addLabels("M2")
            .addLabels("M3")
            .addLabels("M4")
            .setTopN(3)
            .build();
    queryAndVerify(
        facet,
        2,
        LabelAndValue.newBuilder().setLabel("M2").setValue(3).build(),
        LabelAndValue.newBuilder().setLabel("M3").setValue(1).build());
  }

  @Test
  public void testFilteredNonePresent() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field")
            .addLabels("M4")
            .addLabels("M5")
            .addLabels("M6")
            .setTopN(3)
            .build();
    queryAndVerify(facet, 0);
  }

  @Test
  public void testFilteredLessTopN() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field")
            .addLabels("M1")
            .addLabels("M2")
            .setTopN(1)
            .build();
    queryAndVerify(facet, 2, LabelAndValue.newBuilder().setLabel("M2").setValue(3).build());
  }

  @Test
  public void testFilteredSingle() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field_single_valued")
            .addLabels("S1")
            .setTopN(3)
            .build();
    queryAndVerify(facet, 1, LabelAndValue.newBuilder().setLabel("S1").setValue(2).build());
  }

  @Test
  public void testFilteredWithNotPresentSingle() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field_single_valued")
            .addLabels("S2")
            .addLabels("S3")
            .setTopN(3)
            .build();
    queryAndVerify(facet, 1, LabelAndValue.newBuilder().setLabel("S2").setValue(1).build());
  }

  @Test
  public void testFilteredNonePresentSingle() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field_single_valued")
            .addLabels("S3")
            .addLabels("S4")
            .setTopN(3)
            .build();
    queryAndVerify(facet, 0);
  }

  @Test
  public void testFilteredLessTopNSingle() {
    Facet facet =
        Facet.newBuilder()
            .setName("test_facet")
            .setDim("sorted_doc_values_facet_field_single_valued")
            .addLabels("S1")
            .addLabels("S2")
            .setTopN(1)
            .build();
    queryAndVerify(facet, 2, LabelAndValue.newBuilder().setLabel("S1").setValue(2).build());
  }

  private void queryAndVerify(Facet facet, int childCount, LabelAndValue... expectedValues) {
    SearchResponse response = doQuery(DEFAULT_TEST_INDEX, facet);
    assertResponse(response, childCount, expectedValues);
    response = doQuery(MULTI_SEGMENT_INDEX, facet);
    assertResponse(response, childCount, expectedValues);
  }

  private void assertResponse(
      SearchResponse response, int childCount, LabelAndValue... expectedValues) {
    assertEquals(1, response.getFacetResultCount());
    FacetResult result = response.getFacetResult(0);
    assertEquals("test_facet", result.getName());
    assertEquals(childCount, result.getChildCount());
    assertEquals(expectedValues.length, result.getLabelValuesCount());

    for (int i = 0; i < expectedValues.length; ++i) {
      assertEquals(expectedValues[i].getLabel(), result.getLabelValues(i).getLabel());
      assertEquals(expectedValues[i].getValue(), result.getLabelValues(i).getValue(), 0);
    }
  }

  private SearchResponse doQuery(String index, Facet facet) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(index)
                .setStartHit(0)
                .setTopHits(10)
                .addFacets(facet)
                .build());
  }
}
