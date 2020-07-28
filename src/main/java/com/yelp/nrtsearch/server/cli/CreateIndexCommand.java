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
      names = {"-d", "--rootDir"},
      description = "Name of the directory where index is to be created",
      required = true)
  private String rootDir;

  public String getIndexName() {
    return indexName;
  }

  public String getRootDir() {
    return rootDir;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      client.createIndex(getIndexName(), getRootDir());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
