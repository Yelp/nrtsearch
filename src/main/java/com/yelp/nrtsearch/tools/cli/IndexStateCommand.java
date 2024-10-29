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

import com.yelp.nrtsearch.server.grpc.IndexStateRequest;
import com.yelp.nrtsearch.server.grpc.IndexStateResponse;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = IndexStateCommand.INDEX_STATE,
    description = "Get the state for an index")
public class IndexStateCommand implements Callable<Integer> {
  public static final String INDEX_STATE = "indexState";
  private static final Logger logger = LoggerFactory.getLogger(IndexStateCommand.class);

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index to get the state for",
      required = true)
  private String indexName;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      IndexStateResponse response =
          client
              .getBlockingStub()
              .indexState(IndexStateRequest.newBuilder().setIndexName(indexName).build());
      logger.info("Server returned index state: {}", response);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
