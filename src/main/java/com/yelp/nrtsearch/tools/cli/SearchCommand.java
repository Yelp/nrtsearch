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
package com.yelp.nrtsearch.tools.cli;

import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = SearchCommand.SEARCH, description = "Execute a search")
public class SearchCommand implements Callable<Integer> {
  public static final String SEARCH = "search";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description = "Name of the file containing the search to be executed",
      required = true)
  private String fileName;

  public String getFileName() {
    return fileName;
  }

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      Path filePath = Paths.get(getFileName());
      client.search(filePath);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
