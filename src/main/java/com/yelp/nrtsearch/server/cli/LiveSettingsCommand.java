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

  @CommandLine.Option(
      names = {"--sliceMaxDocs"},
      description =
          "Max documents per index slice, or 0 to keep current value. (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private int sliceMaxDocs;

  @CommandLine.Option(
      names = {"--sliceMaxSegments"},
      description =
          "Max segments per index slice, or 0 to keep current value. (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private int sliceMaxSegments;

  @CommandLine.Option(
      names = {"--virtualShards"},
      description =
          "Number of virtual shards to partition index into, or 0 to keep current value. (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private int virtualShards;

  @CommandLine.Option(
      names = {"--maxMergedSegmentMB"},
      description =
          "Max sized segment to produce during normal merging (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private int maxMergedSegmentMB;

  @CommandLine.Option(
      names = {"--segmentsPerTier"},
      description =
          "Number of segments per tier used by TieredMergePolicy, or 0 to keep current value. (default: ${DEFAULT-VALUE})",
      defaultValue = "0")
  private int segmentsPerTier;

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

  public int getSliceMaxDocs() {
    return sliceMaxDocs;
  }

  public int getSliceMaxSegments() {
    return sliceMaxSegments;
  }

  public int getVirtualShards() {
    return virtualShards;
  }

  public int getMaxMergedSegmentMB() {
    return maxMergedSegmentMB;
  }

  public int getSegmentsPerTier() {
    return segmentsPerTier;
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
          getAddDocumentsMaxBufferLen(),
          getSliceMaxDocs(),
          getSliceMaxSegments(),
          getVirtualShards(),
          getMaxMergedSegmentMB(),
          getSegmentsPerTier());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
