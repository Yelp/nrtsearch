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

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LuceneServerClientBuilder<T> {

  T buildRequest(Path filePath) throws IOException;

  class SettingsClientBuilder implements LuceneServerClientBuilder<SettingsRequest> {
    private static final Logger logger =
        LoggerFactory.getLogger(SettingsClientBuilder.class.getName());

    @Override
    public SettingsRequest buildRequest(Path filePath) throws IOException {
      String jsonStr = Files.readString(filePath);
      logger.info(String.format("Converting fields %s to proto SettingsRequest", jsonStr));
      SettingsRequest.Builder settingsRequestBuilder = SettingsRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, settingsRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      // set defaults
      if (settingsRequestBuilder.getNrtCachingDirectoryMaxMergeSizeMB() == 0) {
        settingsRequestBuilder.setNrtCachingDirectoryMaxMergeSizeMB(5.0);
      }
      if (settingsRequestBuilder.getNrtCachingDirectoryMaxSizeMB() == 0) {
        settingsRequestBuilder.setNrtCachingDirectoryMaxSizeMB(60.0);
      }
      if (settingsRequestBuilder.getDirectory().isEmpty()) {
        settingsRequestBuilder.setDirectory("FSDirectory");
      }
      if (settingsRequestBuilder.getNormsFormat().isEmpty()) {
        settingsRequestBuilder.setNormsFormat("Lucene80");
      }
      SettingsRequest settingsRequest = settingsRequestBuilder.build();
      logger.info(
          String.format(
              "jsonStr converted to proto SettingsRequest: \n%s", settingsRequest.toString()));
      return settingsRequest;
    }
  }

  class SettingsV2ClientBuilder implements LuceneServerClientBuilder<SettingsV2Request> {
    private static final Logger logger =
        LoggerFactory.getLogger(SettingsClientBuilder.class.getName());

    @Override
    public SettingsV2Request buildRequest(Path filePath) throws IOException {
      String jsonStr = Files.readString(filePath);
      logger.info(String.format("Converting fields %s to proto IndexSettings", jsonStr));
      IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, indexSettingsBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      SettingsV2Request settingsRequest =
          SettingsV2Request.newBuilder().setSettings(indexSettingsBuilder.build()).build();
      logger.info(
          String.format(
              "jsonStr converted to proto SettingsRequestV2: \n%s",
              JsonFormat.printer().print(settingsRequest)));
      return settingsRequest;
    }
  }

  class StartIndexClientBuilder implements LuceneServerClientBuilder<StartIndexRequest> {
    private static final Logger logger =
        LoggerFactory.getLogger(StartIndexClientBuilder.class.getName());

    @Override
    public StartIndexRequest buildRequest(Path filePath) throws IOException {
      String jsonStr = Files.readString(filePath);
      logger.info(String.format("Converting fields %s to proto StartIndexRequest", jsonStr));
      StartIndexRequest.Builder startIndexRequestBuilder = StartIndexRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, startIndexRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      StartIndexRequest startIndexRequest = startIndexRequestBuilder.build();
      logger.info(
          String.format(
              "jsonStr converted to proto StartIndexRequest: \n%s", startIndexRequest.toString()));
      return startIndexRequest;
    }
  }

  class AddDocumentsClientBuilder implements LuceneServerClientBuilder<Stream<AddDocumentRequest>> {
    private static final Logger logger =
        LoggerFactory.getLogger(AddDocumentsClientBuilder.class.getName());
    private final String indexName;
    private final CSVParser csvParser;

    public AddDocumentsClientBuilder(String indexName, CSVParser csvParser) {
      this.indexName = indexName;
      this.csvParser = csvParser;
    }

    @Override
    public Stream<AddDocumentRequest> buildRequest(Path filePath) {
      Stream.Builder<AddDocumentRequest> builder = Stream.builder();
      AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
      int cnt = 0;
      for (CSVRecord csvRecord : csvParser) {
        { // data rows
          addDocumentRequestBuilder.setIndexName(indexName);
          for (String fieldName : csvParser.getHeaderNames()) {
            String fieldValues = csvRecord.get(fieldName);
            List<String> fieldVals = Arrays.asList(fieldValues.split(";"));
            AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
                AddDocumentRequest.MultiValuedField.newBuilder();
            addDocumentRequestBuilder.putFields(
                fieldName, multiValuedFieldsBuilder.addAllValue(fieldVals).build());
          }
          builder.add(addDocumentRequestBuilder.build());
        }
        logger.info(String.format("Read row %s", cnt));
        cnt++;
      }
      return builder.build();
    }
  }

  class AddJsonDocumentsClientBuilder
      implements LuceneServerClientBuilder<Stream<AddDocumentRequest>> {
    private static final Logger logger =
        LoggerFactory.getLogger(AddJsonDocumentsClientBuilder.class.getName());
    private final String indexName;
    private final Gson gson;
    private final Path filePath;
    private final BufferedReader reader;
    private final int maxBufferLen;
    private boolean finished;

    public AddJsonDocumentsClientBuilder(
        String indexName, Gson gson, Path filePath, int maxBufferLen) throws IOException {
      this.indexName = indexName;
      this.gson = gson;
      this.filePath = filePath;
      this.reader = Files.newBufferedReader(filePath);
      this.maxBufferLen = maxBufferLen;
    }

    private void updateMultiValuedFielduilder(
        AddDocumentRequest.MultiValuedField.Builder builder, Object value) {
      if (value instanceof List) {
        for (Object eachValue : (List) value) {
          updateMultiValuedFielduilder(builder, eachValue);
        }
      } else if (value instanceof Double) {
        Double doubleValue = (Double) value;
        if (doubleValue % 1 == 0) {
          // gson converts all numbers to doubles, but we want to preserve Long
          // e.g. long a=14 becomes 14.0 and Long.of("14.0") fails with NumberFormatException
          builder.addValue(String.valueOf(doubleValue.longValue()));
        } else {
          builder.addValue(String.valueOf(doubleValue));
        }
      } else {
        builder.addValue(String.valueOf(value));
      }
    }

    AddDocumentRequest buildAddDocumentRequest(String lineStr) {
      Map<String, Object> line = gson.fromJson(lineStr, Map.class);
      AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
      addDocumentRequestBuilder.setIndexName(indexName);
      for (String key : line.keySet()) {
        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
            AddDocumentRequest.MultiValuedField.newBuilder();
        Object value = line.get(key);
        if (value != null) {
          updateMultiValuedFielduilder(multiValuedFieldsBuilder, value);
          addDocumentRequestBuilder.putFields(key, multiValuedFieldsBuilder.build());
        }
      }
      return addDocumentRequestBuilder.build();
    }

    @Override
    public Stream<AddDocumentRequest> buildRequest(Path filePath) throws IOException {
      return buildRequest();
    }

    public Stream<AddDocumentRequest> buildRequest() throws IOException {
      Stream.Builder<AddDocumentRequest> builder = Stream.builder();
      for (int i = 0; i < maxBufferLen; i++) {
        String line = reader.readLine();
        if (line == null) {
          finished = true;
          break;
        }
        builder.add(buildAddDocumentRequest(line));
      }
      return builder.build();
    }

    public boolean isFinished() {
      return finished;
    }
  }

  class SearchClientBuilder implements LuceneServerClientBuilder<SearchRequest> {
    private static final Logger logger =
        LoggerFactory.getLogger(SearchClientBuilder.class.getName());

    @Override
    public SearchRequest buildRequest(Path filePath) throws IOException {
      String jsonStr = Files.readString(filePath);
      logger.info(String.format("Converting fields %s to proto SearchRequest", jsonStr));
      SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, searchRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      SearchRequest searchRequest = searchRequestBuilder.build();
      logger.info(
          String.format(
              "jsonStr converted to proto SearchRequest: \n%s", searchRequest.toString()));
      return searchRequest;
    }
  }

  class DeleteDocumentsBuilder implements LuceneServerClientBuilder<AddDocumentRequest> {
    private static final Logger logger =
        LoggerFactory.getLogger(DeleteDocumentsBuilder.class.getName());

    @Override
    public AddDocumentRequest buildRequest(Path filePath) throws IOException {
      String jsonStr = Files.readString(filePath);
      logger.info(String.format("Converting fields %s to proto AddDocumentRequest", jsonStr));
      AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, addDocumentRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      AddDocumentRequest addDocumentRequest = addDocumentRequestBuilder.build();
      logger.info(
          String.format(
              "jsonStr converted to proto AddDocumentRequest: \n%s",
              addDocumentRequest.toString()));
      return addDocumentRequest;
    }
  }
}
