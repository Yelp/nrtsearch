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

import com.google.api.HttpBody;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = MetricsCommand.METRICS,
    description = "Get prometheus metrics for the server")
public class MetricsCommand implements Callable<Integer> {
  public static final String METRICS = "metrics";
  private static final Logger logger = LoggerFactory.getLogger(MetricsCommand.class);

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      HttpBody response = client.getBlockingStub().metrics(Empty.newBuilder().build());
      String metrics = new String(response.getData().toByteArray());
      logger.info("Server returned metrics:\n{}", metrics);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
