/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.tools.cli;

import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.Query;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = DeleteByQueryCommand.DELETE_BY_QUERY,
    description = "Delete all docs that match a query")
public class DeleteByQueryCommand implements Callable<Integer> {
  public static final String DELETE_BY_QUERY = "deleteByQuery";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index whose docs are to be deleted",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"-q", "--query"},
      description = "Deletion query, Query protobuf message as json or @file/path",
      required = true)
  private String query;

  @Override
  public Integer call() throws Exception {
    Query.Builder queryBuilder = Query.newBuilder();
    CliUtils.mergeBuilderFromParam(query, queryBuilder);

    NrtsearchClient client = baseCmd.getClient();
    try {
      client.deleteByQuery(indexName, queryBuilder.build());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
