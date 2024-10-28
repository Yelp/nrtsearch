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

import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = CustomCommand.CUSTOM_COMMAND,
    description = "Sends a custom command to endpoint registered by a plugin")
public class CustomCommand implements Callable<Integer> {
  public static final String CUSTOM_COMMAND = "custom";
  private static final Logger logger = LoggerFactory.getLogger(CustomCommand.class);

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-r", "--request"},
      description =
          "The custom request to send, CustomRequest protobuf message as json or @file/path",
      required = true)
  private String request;

  @Override
  public Integer call() throws Exception {
    CustomRequest.Builder requestBuilder = CustomRequest.newBuilder();
    CliUtils.mergeBuilderFromParam(request, requestBuilder);

    NrtsearchClient client = baseCmd.getClient();
    try {
      CustomResponse response = client.getBlockingStub().custom(requestBuilder.build());
      logger.info("Server returned : {}", response);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
