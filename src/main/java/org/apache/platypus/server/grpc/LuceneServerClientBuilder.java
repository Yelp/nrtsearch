/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.platypus.server.grpc;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface LuceneServerClientBuilder<T> {

    T buildRequest(Path filePath) throws IOException;

    class SettingsClientBuilder implements LuceneServerClientBuilder<SettingsRequest> {
        private static final Logger logger = Logger.getLogger(SettingsClientBuilder.class.getName());

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
            //set defaults
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
            logger.info(String.format("jsonStr converted to proto SettingsRequest: \n%s", settingsRequest.toString()));
            return settingsRequest;
        }
    }

    class StartIndexClientBuilder implements LuceneServerClientBuilder<StartIndexRequest> {
        private static final Logger logger = Logger.getLogger(StartIndexClientBuilder.class.getName());

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
            logger.info(String.format("jsonStr converted to proto StartIndexRequest: \n%s", startIndexRequest.toString()));
            return startIndexRequest;
        }
    }

    class AddDcoumentsClientBuilder implements LuceneServerClientBuilder<Stream<AddDocumentRequest>> {
        private static final Logger logger = Logger.getLogger(AddDcoumentsClientBuilder.class.getName());
        private final String indexName;
        private final CSVParser csvParser;

        public AddDcoumentsClientBuilder(String indexName, CSVParser csvParser) {
            this.indexName = indexName;
            this.csvParser = csvParser;
        }

        @Override
        public Stream<AddDocumentRequest> buildRequest(Path filePath) {
            Stream.Builder<AddDocumentRequest> builder = Stream.builder();
            AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
            int cnt = 0;
            for (CSVRecord csvRecord : csvParser) {
                { //data rows
                    addDocumentRequestBuilder.setIndexName(indexName);
                    for (String fieldName : csvParser.getHeaderNames()) {
                        String fieldValues = csvRecord.get(fieldName);
                        List<String> fieldVals = Arrays.asList(fieldValues.split(";"));
                        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder = AddDocumentRequest.MultiValuedField.newBuilder();
                        addDocumentRequestBuilder.putFields(fieldName, multiValuedFieldsBuilder.addAllValue(fieldVals).build());
                    }
                    builder.add(addDocumentRequestBuilder.build());
                }
                logger.info(String.format("Read row %s", cnt));
                cnt++;
            }
            return builder.build();
        }
    }

    class SearchClientBuilder implements LuceneServerClientBuilder<SearchRequest> {
        private static final Logger logger = Logger.getLogger(SearchClientBuilder.class.getName());

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
            logger.info(String.format("jsonStr converted to proto SearchRequest: \n%s", searchRequest.toString()));
            return searchRequest;
        }
    }


    class DeleteDocumentsBuilder implements LuceneServerClientBuilder<AddDocumentRequest> {
        private static final Logger logger = Logger.getLogger(DeleteDocumentsBuilder.class.getName());

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
            logger.info(String.format("jsonStr converted to proto AddDocumentRequest: \n%s", addDocumentRequest.toString()));
            return addDocumentRequest;
        }
    }
}
