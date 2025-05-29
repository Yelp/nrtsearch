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
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  private static final Map<String, String> INDEX_TO_FIELD_DEF =
      Map.of(
          INDEX_1, "/registerFieldsMultiIndexIndexing1.json",
          INDEX_2, "/registerFieldsMultiIndexIndexing2.json");

  private static final Map<String, String> INDEX_TO_DOCS =
      Map.of(INDEX_1, "/addDocsMultiIndexIndexing1.csv");

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Arrays.asList(INDEX_1, INDEX_2);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    String resourceFile = INDEX_TO_FIELD_DEF.getOrDefault(name, "");
    if (resourceFile.isEmpty()) {
      throw new IllegalStateException("Unexpected index: " + name);
    }
    return getFieldsFromResourceFile(resourceFile);
  }

  @Override
  protected void initIndices() throws Exception {
    for (String indexName : getIndices()) {
      LuceneServerGrpc.LuceneServerBlockingStub blockingStub = getGrpcServer().getBlockingStub();

      // create the index
      blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());

      // Register fields with the same field definition for both indices to ensure compatibility
      FieldDefRequest fieldDefRequest = getIndexDef(INDEX_1);
      blockingStub.registerFields(
          FieldDefRequest.newBuilder(fieldDefRequest).setIndexName(indexName).build());

      // Apply settings and start the index
      blockingStub.liveSettings(getLiveSettings(indexName));
      blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(indexName).build());
    }
  }

  /**
   * Tests adding documents to multiple indices using indexNames list rather than individual
   * indexName.
   */
  @Test
  public void testMultiListIndexAddDocuments() throws Exception {
    // Create documents using the list of target indices
    List<String> targetIndices = Arrays.asList(INDEX_1, INDEX_2);
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
    List<String> targetIndices = Arrays.asList(INDEX_1, INDEX_3);

    try {
      addDocuments(getDocumentRequests(targetIndices));
      fail("Exception not thrown when adding documents to a non-existent index");
    } catch (RuntimeException e) {
      assertThat(e.getCause().getMessage())
          .isEqualTo(
              String.format(
                  "INVALID_ARGUMENT: Index %s does not exist, unable to add documents", INDEX_3));
    }
  }

  /** Creates document requests with multiple index names specified. */
  private Stream<AddDocumentRequest> getDocumentRequests(List<String> indexNames)
      throws URISyntaxException, IOException {
    String docsResourceFile = INDEX_TO_DOCS.get(INDEX_1);
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
      assertThat(response.getHitsList().size()).isEqualTo(3);

      List<String> actualIds =
          response.getHitsList().stream()
              .map(SearchResponse.Hit::getFieldsMap)
              .flatMap(fields -> fields.get("doc_id").getFieldValueList().stream())
              .map(SearchResponse.Hit.FieldValue::getTextValue)
              .collect(Collectors.toList());

      assertThat(actualIds).containsExactlyInAnyOrder("1", "2", "3");
    }
  }

  @Override
  protected LiveSettingsRequest getLiveSettings(String name) {
    return LiveSettingsRequest.newBuilder()
        .setIndexName(name)
        .setAddDocumentsMaxBufferLen(2)
        .build();
  }
}
