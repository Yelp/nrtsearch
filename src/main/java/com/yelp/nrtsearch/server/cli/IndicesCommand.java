/*
 * Copyright 2023 Yelp Inc.
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
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = IndicesCommand.INDICES, description = "Get all indices on the cluster")
public class IndicesCommand implements Callable<Integer> {

  public static final String INDICES = "indices";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      List<String> indicesList = client.getIndices();
      System.out.println();
      if (indicesList.isEmpty()) {
        System.out.println("No index found");
      } else {
        indicesList.forEach(System.out::println);
      }
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
