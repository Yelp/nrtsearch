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

import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = CreateIndexCommand.CREATE_INDEX,
    description = "Creates the index per the specified name")
public class CreateIndexCommand implements Callable<Integer> {
  public static final String CREATE_INDEX = "createIndex";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index to be created",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"--existsWithId"},
      description = "UUID to identify existing state/data to use")
  private String existsWithId;

  @CommandLine.Option(
      names = {"--settings"},
      description = "Initialization of index settings, IndexSettings as json or @file/path")
  private String settings;

  @CommandLine.Option(
      names = {"--liveSettings"},
      description =
          "Initialization of index live settings, IndexLiveSettings as json or @file/path")
  private String liveSettings;

  @CommandLine.Option(
      names = {"--fields"},
      description = "Initialization of index fields, FieldDefRequest as json or @file/path")
  private String fields;

  @CommandLine.Option(
      names = {"--start"},
      description = "If index should also be started using IndexStartConfig")
  private boolean start;

  public String getIndexName() {
    return indexName;
  }

  public String getExistsWithId() {
    return existsWithId;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      IndexSettings indexSettings = null;
      IndexLiveSettings indexLiveSettings = null;
      FieldDefRequest fieldDefRequest = null;
      if (settings != null) {
        indexSettings =
            CliUtils.mergeBuilderFromParam(settings, IndexSettings.newBuilder()).build();
      }
      if (liveSettings != null) {
        indexLiveSettings =
            CliUtils.mergeBuilderFromParam(liveSettings, IndexLiveSettings.newBuilder()).build();
      }
      if (fields != null) {
        fieldDefRequest =
            CliUtils.mergeBuilderFromParam(fields, FieldDefRequest.newBuilder()).build();
      }
      client.createIndex(
          getIndexName(),
          getExistsWithId(),
          indexSettings,
          indexLiveSettings,
          fieldDefRequest,
          start);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
