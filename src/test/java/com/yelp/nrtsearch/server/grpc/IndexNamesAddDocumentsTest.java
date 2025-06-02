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
package com.yelp.nrtsearch.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * This tests adding documents to multiple indices at once by specifying a list of index names
 * rather than setting the index name individually for each document.
 */
public class IndexNamesAddDocumentsTest extends ServerTestCase {
  private static final String INDEX_1 = "test_index_1";
  private static final String INDEX_2 = "test_index_2";
  private static final String INDEX_3 = "test_index_3";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Arrays.asList(INDEX_1, INDEX_2);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return FieldDefRequest.newBuilder(
            getFieldsFromResourceFile("/registerFieldsMultiIndexIndexing1.json"))
        .setIndexName(name)
        .build();
  }

  /**
   * Tests adding documents to multiple indices using indexNames list rather than individual
   * indexName.
   */
  @Test
  public void testMultiIndexSameRequestAddDocuments() throws Exception {
    // Create documents using the list of target indices
    List<String> targetIndices = List.of(INDEX_1, INDEX_2);
    Stream<AddDocumentRequest> requestStream = getDocumentRequests(targetIndices);

    // Add documents and refresh indices
    addDocuments(requestStream);
    refreshIndices(targetIndices);

    // Verify documents were added to both indices
    verifyIndicesContainDocuments(targetIndices);
  }

  /** Tests that an error occurs when trying to add documents to a non-existent index. */
  @Test
  public void testNonExistentIndexInListThrowsException() throws Exception {
    List<String> targetIndices = List.of(INDEX_1, INDEX_3);

    try {
      addDocuments(getDocumentRequests(targetIndices));
      fail("Exception not thrown when adding documents to a non-existent index");
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      assertThat(cause).isInstanceOf(StatusRuntimeException.class);
      assertThat(cause.getMessage())
          .isEqualTo(
              String.format(
                  "INVALID_ARGUMENT: Index %s does not exist, unable to add documents", INDEX_3));
    }
  }

  /** Tests that an error occurs when neither indexName nor indexNames are specified. */
  @Test
  public void testNeitherIndexNameNorIndexNamesSpecifiedThrowsException() throws Exception {
    try {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .putFields(
                  "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
              .putFields(
                  "text_field",
                  AddDocumentRequest.MultiValuedField.newBuilder().addValue("test text").build())
              .build();

      addDocuments(Stream.of(request));
      fail("Exception not thrown when neither indexName nor indexNames are specified");
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      assertThat(cause).isInstanceOf(StatusRuntimeException.class);
      assertThat(cause.getMessage())
          .contains("Must provide exactly one of indexName or indexNames but neither is set");
    }
  }

  /** Tests that an error occurs when both indexName and indexNames are specified. */
  @Test
  public void testBothIndexNameAndIndexNamesSpecifiedThrowsException() throws Exception {
    try {
      AddDocumentRequest request =
          AddDocumentRequest.newBuilder()
              .setIndexName(INDEX_1)
              .addAllIndexNames(List.of(INDEX_1, INDEX_2))
              .putFields(
                  "doc_id", AddDocumentRequest.MultiValuedField.newBuilder().addValue("1").build())
              .putFields(
                  "text_field",
                  AddDocumentRequest.MultiValuedField.newBuilder().addValue("test text").build())
              .build();

      addDocuments(Stream.of(request));
      fail("Exception not thrown when both indexName and indexNames are specified");
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      assertThat(cause).isInstanceOf(StatusRuntimeException.class);
      assertThat(cause.getMessage())
          .contains("Must provide exactly one of indexName or indexNames but both are set");
    }
  }

  /** Creates document requests with multiple index names specified. */
  private Stream<AddDocumentRequest> getDocumentRequests(List<String> indexNames)
      throws URISyntaxException, IOException {
    String docsResourceFile = "/addDocsMultiIndexIndexing1.csv";
    Path filePath = Paths.get(ServerTestCase.class.getResource(docsResourceFile).toURI());
    Reader reader = Files.newBufferedReader(filePath);
    CSVParser csvParser =
        new CSVParser(
            reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());

    return new NrtsearchClientBuilder.AddDocumentsClientBuilder(indexNames, csvParser)
        .buildRequest(filePath);
  }

  /** Refreshes the specified indices to make documents searchable. */
  private void refreshIndices(List<String> indexNames) {
    for (String indexName : indexNames) {
      getGrpcServer()
          .getBlockingStub()
          .refresh(RefreshRequest.newBuilder().setIndexName(indexName).build());
    }
  }

  /** Verifies that each index contains the expected documents. */
  private void verifyIndicesContainDocuments(List<String> indexNames) {
    for (String indexName : indexNames) {
      SearchResponse response =
          getGrpcServer()
              .getBlockingStub()
              .search(
                  SearchRequest.newBuilder()
                      .setIndexName(indexName)
                      .setTopHits(1000)
                      .addAllRetrieveFields(List.of("doc_id"))
                      .build());

      assertThat(response.getHitsCount()).isEqualTo(3);

      List<String> actualIds =
          response.getHitsList().stream()
              .map(SearchResponse.Hit::getFieldsMap)
              .flatMap(fields -> fields.get("doc_id").getFieldValueList().stream())
              .map(SearchResponse.Hit.FieldValue::getTextValue)
              .collect(Collectors.toList());

      assertThat(actualIds).containsExactlyInAnyOrder("1", "2", "3");
    }
  }

  /**
   * Setting AddDocumentsMaxBufferLen to 2 so that 2 documents are indexed after queue is full and 1
   * document after there are no more documents, but queue is not full. This is so that we can test
   * the entire flow of the add documents handler 1. Tests batch processing when buffer fills (first
   * 2 docs) - this triggers immediate indexing 2. Tests final processing of remainder docs (3rd
   * doc) - processed during onCompleted() 3. Ensures both indexing code paths in AddDocumentHandler
   * are exercised in a single test
   */
  @Override
  protected LiveSettingsRequest getLiveSettings(String name) {
    return LiveSettingsRequest.newBuilder()
        .setIndexName(name)
        .setAddDocumentsMaxBufferLen(2)
        .build();
  }
}
