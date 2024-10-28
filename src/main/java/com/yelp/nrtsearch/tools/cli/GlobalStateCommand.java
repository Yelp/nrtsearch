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

import com.yelp.nrtsearch.server.grpc.GlobalStateRequest;
import com.yelp.nrtsearch.server.grpc.GlobalStateResponse;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = GlobalStateCommand.GLOBAL_STATE,
    description = "Get server global state")
public class GlobalStateCommand implements Callable<Integer> {
  public static final String GLOBAL_STATE = "globalState";
  private static final Logger logger = LoggerFactory.getLogger(GlobalStateCommand.class);

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      GlobalStateResponse response =
          client.getBlockingStub().globalState(GlobalStateRequest.newBuilder().build());
      logger.info("Server returned global state: {}", response);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
