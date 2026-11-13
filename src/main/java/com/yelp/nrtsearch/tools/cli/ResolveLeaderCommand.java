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

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderRequest;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

/**
 * Ask a node for the active primary it knows. Prints the response as a single line of JSON
 * (including default values) on stdout, or {@code {"error":"<status code>","description":...}} and
 * exits with 2 if the call fails, for use in scripts.
 */
@CommandLine.Command(
    name = ResolveLeaderCommand.RESOLVE_LEADER,
    description = "Get the active primary known to a node, as one line of JSON")
public class ResolveLeaderCommand implements Callable<Integer> {
  public static final String RESOLVE_LEADER = "resolveLeader";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"--timeoutMs"},
      description = "Call deadline in ms (default: ${DEFAULT-VALUE})",
      defaultValue = "3000")
  private long timeoutMs;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      ResolveLeaderResponse response =
          client
              .getBlockingStub()
              .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
              .resolveLeader(ResolveLeaderRequest.newBuilder().build());
      System.out.println(
          JsonFormat.printer()
              .includingDefaultValueFields()
              .omittingInsignificantWhitespace()
              .print(response));
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(CliUtils.errorJson(e));
      return 2;
    } finally {
      client.shutdown();
    }
  }
}
