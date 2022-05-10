/*
 * Copyright 2022 Yelp Inc.
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
    name = ReadyCommand.READY,
    description = "Check if the given indices are ready")
public class ReadyCommand implements Callable<Integer> {
  public static final String READY = "ready";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indices"},
      description =
          "Comma separated list of indices to check if ready, or empty to check all indices",
      defaultValue = "")
  private String indices;

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    boolean ready;
    try {
      ready = client.ready(indices);
    } finally {
      client.shutdown();
    }
    return ready ? 0 : 1;
  }
}
