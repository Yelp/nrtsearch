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
    System.exit(new CommandLine(new VectorBenchCommand()).execute(args));
  }

  @Override
  public void run() {
    new CommandLine(this).execute("help");
  }
}
