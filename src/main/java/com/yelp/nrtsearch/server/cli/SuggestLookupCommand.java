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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = SuggestLookupCommand.SUGGEST_LOOKUP,
    description = "Lookup suggest index (only CompletionInfixSuggester is supported in the cli)")
public class SuggestLookupCommand implements Callable<Integer> {

  public static final String SUGGEST_LOOKUP = "suggestLookup";

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
      names = {"--suggestText"},
      description = "Text used for generating suggestions",
      required = true)
  private String suggestText;

  private String getSuggestText() {
    return suggestText;
  }

  @CommandLine.Option(
      names = {"--contexts"},
      description = "Context array used for boosting suggest ranking. Context array example: c1#c2",
      defaultValue = "")
  private String contexts;

  private List<String> getContexts() {
    String[] contextsArray = contexts.split("#");
    if (contextsArray.length == 0) {
      return List.of();
    }
    return Arrays.asList(contextsArray);
  }

  @CommandLine.Option(
      names = {"--count"},
      description = "Maximum count of suggestions to return",
      defaultValue = "")
  private String count;

  private int getCount() {
    return Integer.parseInt(count);
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      String indexName = getIndexName();
      String suggestName = getSuggestName();
      String suggestText = getSuggestText();
      List<String> contexts = getContexts();
      int count = getCount();
      client.suggestLookup(indexName, suggestName, suggestText, contexts, count);
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
