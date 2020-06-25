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
    name = DeleteDocumentsCommand.DELETE_DOCS,
    mixinStandardHelpOptions = true,
    version = "delete 0.1",
    description = "Delete documents from index that match any terms")
public class DeleteDocumentsCommand {
  public static final String DELETE_DOCS = "delete";

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "name of the index whose docs are to be deleted",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"-f", "--fileName"},
      description = "file containing document filters to be deleted",
      required = true)
  private String fileName;

  public String getFileName() {
    return fileName;
  }
}
