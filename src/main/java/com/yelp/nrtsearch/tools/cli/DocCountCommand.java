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

import com.yelp.nrtsearch.server.grpc.MatchAllQuery;
import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

/**
 * Count all documents of an index with an exact match all search. Prints {@code
 * {"totalHits":N,"searcherVersion":V}} as a single line of JSON on stdout, or {@code
 * {"error":"<status code>","description":...}} and exits with 2 if the call fails, for use in
 * scripts.
 */
@CommandLine.Command(
    name = DocCountCommand.DOC_COUNT,
    description = "Exact count of all documents in an index, as one line of JSON")
public class DocCountCommand implements Callable<Integer> {
  public static final String DOC_COUNT = "docCount";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Index to count",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"--timeoutMs"},
      description = "Call deadline in ms (default: ${DEFAULT-VALUE})",
      defaultValue = "10000")
  private long timeoutMs;

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      SearchResponse response =
          client
              .getBlockingStub()
              .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
              .search(
                  SearchRequest.newBuilder()
                      .setIndexName(indexName)
                      .setTopHits(0)
                      .setTotalHitsThreshold(Integer.MAX_VALUE)
                      .setQuery(
                          Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder().build()))
                      .build());
      System.out.printf(
          "{\"totalHits\":%d,\"searcherVersion\":%d}%n",
          response.getTotalHits().getValue(), response.getSearchState().getSearcherVersion());
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(CliUtils.errorJson(e));
      return 2;
    } finally {
      client.shutdown();
    }
  }
}
