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
package com.yelp.nrtsearch.server.luceneserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.LuceneServerClientBuilder;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * This tests adding documents to multiple indices in a single addDocuments request where the
 * documents may be in any order.
 */
public class MultiIndexAddDocumentsTest extends ServerTestCase {
  private static final String INDEX_1 = "test_index_1";
  private static final String INDEX_2 = "test_index_2";
  private static final String INDEX_3 = "test_index_3";

  private static final Map<String, String> INDEX_TO_FIELD_DEF =
      Map.of(
          INDEX_1,
          "/registerFieldsMultiIndexIndexing1.json",
          INDEX_2,
          "/registerFieldsMultiIndexIndexing2.json");
  private static final Map<String, String> INDEX_TO_DOCS =
      Map.of(
          INDEX_1, "/addDocsMultiIndexIndexing1.csv", INDEX_2, "/addDocsMultiIndexIndexing2.csv");

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

      // register fields
      blockingStub.registerFields(getIndexDef(indexName));

      // apply live settings
      blockingStub.liveSettings(getLiveSettings(indexName));

      // start the index
      StartIndexRequest.Builder startIndexBuilder =
          StartIndexRequest.newBuilder().setIndexName(indexName);
      blockingStub.startIndex(startIndexBuilder.build());
    }

    // add Docs
    addDocuments();

    // refresh
    for (String indexName : getIndices()) {
      getGrpcServer()
          .getBlockingStub()
          .refresh(RefreshRequest.newBuilder().setIndexName(indexName).build());
    }
  }

  /**
   * This adds documents to the indices in a single request where the documents may be in any order.
   */
  public void addDocuments() throws Exception {
    List<AddDocumentRequest> allRequests = new ArrayList<>();
    allRequests.addAll(getAddDocumentRequestStream(INDEX_1).collect(Collectors.toList()));
    allRequests.addAll(getAddDocumentRequestStream(INDEX_2).collect(Collectors.toList()));
    Collections.shuffle(allRequests);
    addDocuments(allRequests.stream());
  }

  private Stream<AddDocumentRequest> getAddDocumentRequestStream(String index)
      throws URISyntaxException, IOException {
    String docsResourceFile = INDEX_TO_DOCS.get(index);
    Path filePath = Paths.get(ServerTestCase.class.getResource(docsResourceFile).toURI());
    Reader reader = Files.newBufferedReader(filePath);
    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
    return new LuceneServerClientBuilder.AddDocumentsClientBuilder(index, csvParser)
        .buildRequest(filePath);
  }

  @Test
  public void testMultiIndexIndexing() {
    SearchResponse searchResponse =
        getGrpcServer().getBlockingStub().search(getMatchAllDocsQuery(INDEX_1));
    assertIdsInResponse(searchResponse, "1", "2", "3");

    searchResponse = getGrpcServer().getBlockingStub().search(getMatchAllDocsQuery(INDEX_2));
    assertIdsInResponse(searchResponse, "7", "8", "9");
  }

  @Test
  public void testIndexDoesNotExist() throws Exception {
    List<AddDocumentRequest> allRequests =
        getAddDocumentRequestStream(INDEX_1).collect(Collectors.toList());
    allRequests.add(
        AddDocumentRequest.newBuilder()
            .setIndexName(INDEX_3)
            .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
            .build());
    Collections.shuffle(allRequests);

    try {
      addDocuments(allRequests.stream());
      fail("Exception not thrown when adding documents");
    } catch (RuntimeException e) {
      assertThat(e.getCause().getMessage())
          .isEqualTo(
              String.format(
                  "INVALID_ARGUMENT: Index %s does not exist, unable to add documents", INDEX_3));
    }
  }

  private SearchRequest getMatchAllDocsQuery(String index) {
    return SearchRequest.newBuilder()
        .setIndexName(index)
        .setTopHits(1000)
        .addAllRetrieveFields(List.of("doc_id"))
        .build();
  }

  private void assertIdsInResponse(SearchResponse response, String... expectedIds) {
    assertThat(response.getHitsCount()).isEqualTo(expectedIds.length);
    assertThat(response.getHitsList().size()).isEqualTo(expectedIds.length);
    List<String> actualIds =
        response.getHitsList().stream()
            .map(SearchResponse.Hit::getFieldsMap)
            .flatMap(fields -> fields.get("doc_id").getFieldValueList().stream())
            .map(SearchResponse.Hit.FieldValue::getTextValue)
            .collect(Collectors.toList());
    assertThat(actualIds).containsExactlyInAnyOrder(expectedIds);
  }

  /**
   * Setting AddDocumentsMaxBufferLen to 2 so that 2 documents are indexed after queue is full and 1
   * document after there are no more documents but queue is not full.
   */
  @Override
  protected LiveSettingsRequest getLiveSettings(String name) {
    return LiveSettingsRequest.newBuilder()
        .setIndexName(name)
        .setAddDocumentsMaxBufferLen(2)
        .build();
  }
}
