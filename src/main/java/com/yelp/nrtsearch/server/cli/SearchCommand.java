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
package com.yelp.nrtsearch.server.cli;

import picocli.CommandLine;

@CommandLine.Command(
    name = SearchCommand.SEARCH,
    mixinStandardHelpOptions = true,
    version = "search 0.1",
    description = "Execute a search")
public class SearchCommand {
  public static final String SEARCH = "search";

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description = "name of the file containing the search to be executed",
      required = true)
  private String fileName;

  public String getFileName() {
    return fileName;
  }
}
