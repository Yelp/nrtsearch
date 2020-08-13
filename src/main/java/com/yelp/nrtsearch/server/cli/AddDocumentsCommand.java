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
package com.yelp.nrtsearch.server.cli;

import static com.yelp.nrtsearch.server.cli.AddDocumentsCommand.ADD_DOCUMENTS;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import com.yelp.nrtsearch.server.grpc.LuceneServerClientBuilder;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import picocli.CommandLine;

@CommandLine.Command(name = ADD_DOCUMENTS, description = "Add documents to the index")
public class AddDocumentsCommand implements Callable<Integer> {
  public static final String ADD_DOCUMENTS = "addDocuments";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description =
          "Documents to be added in a csv or json format. For csv first row has names of fields and rows after are values",
      required = true)
  private String fileName;

  public String getFileName() {
    return fileName;
  }

  @CommandLine.Option(
      names = {"-t", "--fileType"},
      description = "Type of input file: ('csv' | 'json')",
      required = true)
  private String fileType;

  public String getFileType() {
    return fileType;
  }

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index to add documents to",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"-l", "--maxBufferLen"},
      description =
          "Num Docs to batch up as one stream sent to the server (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  private String maxBufferLen;

  public int getMaxBufferLen() {
    return Integer.parseInt(maxBufferLen);
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      String indexName = getIndexName();
      String fileType = getFileType();
      Stream<AddDocumentRequest> addDocumentRequestStream;
      Path filePath = Paths.get(getFileName());
      if (fileType.equalsIgnoreCase("csv")) {
        Reader reader = Files.newBufferedReader(filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        addDocumentRequestStream =
            new LuceneServerClientBuilder.AddDocumentsClientBuilder(indexName, csvParser)
                .buildRequest(filePath);
        client.addDocuments(addDocumentRequestStream);
      } else if (fileType.equalsIgnoreCase("json")) {
        LuceneServerClientBuilder.AddJsonDocumentsClientBuilder addJsonDocumentsClientBuilder =
            new LuceneServerClientBuilder.AddJsonDocumentsClientBuilder(
                indexName, new Gson(), filePath, getMaxBufferLen());
        while (!addJsonDocumentsClientBuilder.isFinished()) {
          addDocumentRequestStream = addJsonDocumentsClientBuilder.buildRequest(filePath);
          client.addDocuments(addDocumentRequestStream);
        }
      } else {
        throw new RuntimeException(String.format("%s is not a valid fileType", fileType));
      }
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
