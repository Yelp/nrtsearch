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

import static com.yelp.nrtsearch.tools.cli.StatusCommand.STATUS;

import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = STATUS, description = "Get current status of a host")
public class StatusCommand implements Callable<Integer> {
  public static final String STATUS = "status";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      client.status();
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
