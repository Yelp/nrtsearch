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

import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = BuildSuggestCommand.BUILD_SUGGEST,
    description = "Build suggest index (only CompletionInfixSuggester is supported in the cli)")
public class BuildSuggestCommand implements Callable<Integer> {
  public static final String BUILD_SUGGEST = "buildSuggest";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"--suggestName"},
      description = "Suggest index name",
      required = true)
  private String suggestName;

  private String getSuggestName() {
    return suggestName;
  }

  @CommandLine.Option(
      names = {"--indexName"},
      description = "Original index name",
      required = true)
  private String indexName;

  private String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"--analyzer"},
      description = "Analyzer name",
      defaultValue = "analyzer")
  private String analyzer;

  private String getAnalyzer() {
    return analyzer;
  }

  @CommandLine.Option(
      names = {"--indexGen"},
      description = "Index gen number",
      required = true)
  private String indexGen;

  private long getIndexGen() throws NumberFormatException {
    return Long.parseLong(indexGen);
  }

  @CommandLine.Option(
      names = {"--suggestFieldName"},
      description = "Name of suggest field in index",
      defaultValue = "text")
  private String suggestFieldName;

  private String getSuggestFieldName() {
    return suggestFieldName;
  }

  @CommandLine.Option(
      names = {"--weightFieldName"},
      description = "Name of weight field in index",
      defaultValue = "weight")
  private String weightFieldName;

  private String getWeightFieldName() {
    return weightFieldName;
  }

  @CommandLine.Option(
      names = {"--payloadFieldName"},
      description = "Name of payload field in index",
      defaultValue = "payload")
  private String payloadFieldName;

  private String getPayloadFieldName() {
    return payloadFieldName;
  }

  @CommandLine.Option(
      names = {"--contextFieldName"},
      description = "Name of context field in index",
      defaultValue = "context")
  private String contextFieldName;

  private String getContextFieldName() {
    return contextFieldName;
  }

  @CommandLine.Option(
      names = {"--suggestTextFieldName"},
      description = "Name of suggest text field in index",
      defaultValue = "search_text")
  private String suggestTextFieldName;

  private String getSuggestTextFieldName() {
    return suggestTextFieldName;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      String suggestName = getSuggestName();
      String indexName = getIndexName();
      String analyzer = getAnalyzer();
      long indexGen = getIndexGen();
      String suggestFieldName = getSuggestFieldName();
      String weightFieldName = getWeightFieldName();
      String payloadFieldName = getPayloadFieldName();
      String contextFieldName = getContextFieldName();
      String suggestTextFieldName = getSuggestTextFieldName();

      client.buildSuggestIndex(
          suggestName,
          indexName,
          analyzer,
          indexGen,
          suggestFieldName,
          weightFieldName,
          payloadFieldName,
          contextFieldName,
          suggestTextFieldName);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
