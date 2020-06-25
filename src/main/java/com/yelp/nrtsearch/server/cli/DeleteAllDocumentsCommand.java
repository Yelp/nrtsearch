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
    name = DeleteAllDocumentsCommand.DELETE_ALL_DOCS,
    mixinStandardHelpOptions = true,
    version = "deleteAllDocs 0.1",
    description = "Delete all docs in the index")
public class DeleteAllDocumentsCommand {
  public static final String DELETE_ALL_DOCS = "deleteAllDocs";

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "name of the index whose docs are to be deleted",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }
}
