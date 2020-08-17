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
    name = LiveSettingsCommand.LIVE_SETTINGS,
    description = "Updates the lives settings for the the specified index")
public class LiveSettingsCommand implements Callable<Integer> {
  public static final String LIVE_SETTINGS = "liveSettings";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index whose live settings are to be updated",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"--maxRefreshSec"},
      description =
          "Longest time to wait before reopening IndexSearcher (i.e., periodic background reopen). (default: ${DEFAULT-VALUE})",
      defaultValue = "1.0")
  private double maxRefreshSec;

  @CommandLine.Option(
      names = {"--minRefreshSec"},
      description =
          "Shortest time to wait before reopening IndexSearcher (i.e., when a search is waiting for a specific indexGen). (default: ${DEFAULT-VALUE})",
      defaultValue = "0.5")
  private double minRefreshSec;

  @CommandLine.Option(
      names = {"--maxSearcherAgeSec"},
      description = "Non-current searchers older than this are pruned. (default: ${DEFAULT-VALUE})",
      defaultValue = "60.0")
  private double maxSearcherAgeSec;

  @CommandLine.Option(
      names = {"--indexRamBufferSizeMB"},
      description = "Size (in MB) of IndexWriter's RAM buffer. (default: ${DEFAULT-VALUE})",
      defaultValue = "250")
  private double indexRamBufferSizeMB;

  @CommandLine.Option(
      names = {"--addDocumentsMaxBufferLen"},
      description = "Max number of documents to add at a time. (default: ${DEFAULT-VALUE})",
      defaultValue = "100")
  private int addDocumentsMaxBufferLen;

  public String getIndexName() {
    return indexName;
  }

  public double getMaxRefreshSec() {
    return maxRefreshSec;
  }

  public double getMinRefreshSec() {
    return minRefreshSec;
  }

  public double getMaxSearcherAgeSec() {
    return maxSearcherAgeSec;
  }

  public double getIndexRamBufferSizeMB() {
    return indexRamBufferSizeMB;
  }

  public int getAddDocumentsMaxBufferLen() {
    return addDocumentsMaxBufferLen;
  }

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      client.liveSettings(
          getIndexName(),
          getMaxRefreshSec(),
          getMinRefreshSec(),
          getMaxSearcherAgeSec(),
          getIndexRamBufferSizeMB(),
          getAddDocumentsMaxBufferLen());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
