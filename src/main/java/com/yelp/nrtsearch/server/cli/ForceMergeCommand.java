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

import com.yelp.nrtsearch.server.grpc.ForceMergeRequest;
import com.yelp.nrtsearch.server.grpc.ForceMergeResponse;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = ForceMergeCommand.FORCE_MERGE, description = "Force merge")
public class ForceMergeCommand implements Callable<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(ForceMergeCommand.class);
  public static final String FORCE_MERGE = "forceMerge";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Index whose segments must be force merged",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"-m", "--maxNumSegments"},
      description = "Maximum number of segments after force merge",
      required = true)
  private int maxNumSegments;

  public int getMaxNumSegments() {
    return maxNumSegments;
  }

  @CommandLine.Option(
      names = {"-d", "--doWait"},
      description =
          "If true, waits until the force merge is completed before returning a response. Otherwise starts force merging in async and returns a response.",
      required = true)
  private boolean doWait;

  public boolean getDoWait() {
    return doWait;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      ForceMergeResponse response = client.getBlockingStub().forceMerge(
              ForceMergeRequest.newBuilder()
                      .setIndexName(getIndexName())
                      .setMaxNumSegments(getMaxNumSegments())
                      .setDoWait(getDoWait())
                      .build()
      );
      logger.info("Force merge response: {}", response.getStatus());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
