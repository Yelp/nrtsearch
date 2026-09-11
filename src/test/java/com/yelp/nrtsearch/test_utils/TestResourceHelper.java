/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.test_utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.NrtsearchClientBuilder;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class TestResourceHelper {

  public static FieldDefRequest getFieldsFromResourceFile(String resourceFileName)
      throws IOException {
    InputStream fileStream = TestResourceHelper.class.getResourceAsStream(resourceFileName);
    if (fileStream == null) {
      throw new IOException("Resource not found: " + resourceFileName);
    }
    String jsonText =
        new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining(System.lineSeparator()));
    return getFieldsFromJson(jsonText);
  }

  public static FieldDefRequest getFieldsFromJson(String jsonStr) {
    FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return fieldDefRequestBuilder.build();
  }

  public static Stream<AddDocumentRequest> getCsvDocumentStream(
      String indexName, String resourceFileName) throws IOException, URISyntaxException {
    Path filePath = Paths.get(TestResourceHelper.class.getResource(resourceFileName).toURI());
    Reader reader = Files.newBufferedReader(filePath);
    CSVParser csvParser =
        new CSVParser(
            reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
    return new NrtsearchClientBuilder.AddDocumentsClientBuilder(indexName, csvParser)
        .buildRequest(filePath);
  }

  public static SearchRequest getSearchRequestFromResourceFile(String resourceFileName)
      throws IOException {
    InputStream fileStream = TestResourceHelper.class.getResourceAsStream(resourceFileName);
    if (fileStream == null) {
      throw new IOException("Resource not found: " + resourceFileName);
    }
    String jsonText =
        new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining(System.lineSeparator()));
    SearchRequest.Builder builder = SearchRequest.newBuilder();
    try {
      JsonFormat.parser().merge(jsonText, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }
}
