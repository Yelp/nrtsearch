/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.tools.vector_bench;

import picocli.CommandLine;

/**
 * Load and query tools for testing vector indexes, installed as the {@code vector_bench} script.
 */
@CommandLine.Command(
    name = "vector_bench",
    synopsisSubcommandLabel = "COMMAND",
    description = "Create, load, stream to and query a test vector index.",
    subcommands = {
      SetupCommand.class,
      LoadCommand.class,
      StreamCommand.class,
      GroundTruthCommand.class,
      QueryCommand.class,
      InfoCommand.class,
      CommandLine.HelpCommand.class
    })
public class VectorBenchCommand implements Runnable {

  public static void main(String[] args) {
    // gRPC logs startup details such as epoll support at INFO through java.util.logging
    java.util.logging.Logger.getLogger("io.grpc").setLevel(java.util.logging.Level.WARNING);
    CommandLine commandLine = new CommandLine(new VectorBenchCommand());
    commandLine.setExecutionExceptionHandler(VectorBenchCommand::handleError);
    System.exit(commandLine.execute(args));
  }

  /** Print a one line message for errors a user can act on, and a stack trace otherwise. */
  static int handleError(
      Exception e, CommandLine commandLine, CommandLine.ParseResult parseResult) {
    Throwable cause = e;
    while (cause.getCause() != null
        && !(cause instanceof io.grpc.StatusRuntimeException)
        && !(cause instanceof BenchClient.BenchException)) {
      cause = cause.getCause();
    }
    if (cause instanceof io.grpc.StatusRuntimeException sre
        && sre.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE) {
      commandLine
          .getErr()
          .println(
              "error: can't connect to nrtsearch ("
                  + sre.getStatus().getDescription()
                  + "). Check the node is running and --host/--port are right"
                  + " (cluster.sh status).");
      return 1;
    }
    if (cause instanceof io.grpc.StatusRuntimeException sre) {
      commandLine.getErr().println("error: " + sre.getStatus());
      return 1;
    }
    if (cause instanceof BenchClient.BenchException) {
      commandLine.getErr().println("error: " + cause.getMessage());
      return 1;
    }
    e.printStackTrace(commandLine.getErr());
    return 1;
  }

  @Override
  public void run() {
    new CommandLine(this).execute("help");
  }
}
