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

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.UInt64Value;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.LiveSettingsV2Request;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = LiveSettingsV2Command.LIVE_SETTINGS_V2,
    description = "Updates the lives settings for the the specified index")
public class LiveSettingsV2Command implements Callable<Integer> {
  public static final String LIVE_SETTINGS_V2 = "liveSettingsV2";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "Name of the index whose live settings are to be updated",
      required = true)
  private String indexName;

  @CommandLine.Option(
      names = {"--maxRefreshSec"},
      description =
          "Longest time to wait before reopening IndexSearcher (i.e., periodic background reopen)")
  private Double maxRefreshSec;

  @CommandLine.Option(
      names = {"--minRefreshSec"},
      description =
          "Shortest time to wait before reopening IndexSearcher (i.e., when a search is waiting for a specific indexGen)")
  private Double minRefreshSec;

  @CommandLine.Option(
      names = {"--maxSearcherAgeSec"},
      description = "Non-current searchers older than this are pruned")
  private Double maxSearcherAgeSec;

  @CommandLine.Option(
      names = {"--indexRamBufferSizeMB"},
      description = "Size (in MB) of IndexWriter's RAM buffer")
  private Double indexRamBufferSizeMB;

  @CommandLine.Option(
      names = {"--addDocumentsMaxBufferLen"},
      description = "Max number of documents to add at a time")
  private Integer addDocumentsMaxBufferLen;

  @CommandLine.Option(
      names = {"--sliceMaxDocs"},
      description = "Max documents per index slice")
  private Integer sliceMaxDocs;

  @CommandLine.Option(
      names = {"--sliceMaxSegments"},
      description = "Max segments per index slice")
  private Integer sliceMaxSegments;

  @CommandLine.Option(
      names = {"--virtualShards"},
      description = "Number of virtual shards to partition index into")
  private Integer virtualShards;

  @CommandLine.Option(
      names = {"--maxMergedSegmentMB"},
      description = "Max sized segment to produce during normal merging")
  private Integer maxMergedSegmentMB;

  @CommandLine.Option(
      names = {"--segmentsPerTier"},
      description = "Number of segments per tier used by TieredMergePolicy")
  private Integer segmentsPerTier;

  @CommandLine.Option(
      names = {"--defaultSearchTimeoutSec"},
      description = "Search timeout to use when not provided by the request")
  private Double defaultSearchTimeoutSec;

  @CommandLine.Option(
      names = {"--defaultSearchTimeoutCheckEvery"},
      description = "Timeout check every value to use when not provided by the request")
  private Integer defaultSearchTimeoutCheckEvery;

  @CommandLine.Option(
      names = {"--defaultTerminateAfter"},
      description = "Terminate after to use when not provided by the request")
  private Integer defaultTerminateAfter;

  @CommandLine.Option(
      names = {"--maxMergePreCopyDurationSec"},
      description = "Maximum time allowed for merge precopy in seconds")
  private Long maxMergePreCopyDurationSec;

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      LiveSettingsV2Request.Builder settingsRequestV2Builder = LiveSettingsV2Request.newBuilder();
      settingsRequestV2Builder.setIndexName(indexName);
      IndexLiveSettings.Builder liveSettingsBuilder = IndexLiveSettings.newBuilder();
      if (maxRefreshSec != null) {
        liveSettingsBuilder.setMaxRefreshSec(
            DoubleValue.newBuilder().setValue(maxRefreshSec).build());
      }
      if (minRefreshSec != null) {
        liveSettingsBuilder.setMinRefreshSec(
            DoubleValue.newBuilder().setValue(minRefreshSec).build());
      }
      if (maxSearcherAgeSec != null) {
        liveSettingsBuilder.setMaxSearcherAgeSec(
            DoubleValue.newBuilder().setValue(maxSearcherAgeSec).build());
      }
      if (indexRamBufferSizeMB != null) {
        liveSettingsBuilder.setIndexRamBufferSizeMB(
            DoubleValue.newBuilder().setValue(indexRamBufferSizeMB).build());
      }
      if (addDocumentsMaxBufferLen != null) {
        liveSettingsBuilder.setAddDocumentsMaxBufferLen(
            Int32Value.newBuilder().setValue(addDocumentsMaxBufferLen).build());
      }
      if (sliceMaxDocs != null) {
        liveSettingsBuilder.setSliceMaxDocs(Int32Value.newBuilder().setValue(sliceMaxDocs).build());
      }
      if (sliceMaxSegments != null) {
        liveSettingsBuilder.setSliceMaxSegments(
            Int32Value.newBuilder().setValue(sliceMaxSegments).build());
      }
      if (virtualShards != null) {
        liveSettingsBuilder.setVirtualShards(
            Int32Value.newBuilder().setValue(virtualShards).build());
      }
      if (maxMergedSegmentMB != null) {
        liveSettingsBuilder.setMaxMergedSegmentMB(
            Int32Value.newBuilder().setValue(maxMergedSegmentMB).build());
      }
      if (segmentsPerTier != null) {
        liveSettingsBuilder.setSegmentsPerTier(
            Int32Value.newBuilder().setValue(segmentsPerTier).build());
      }
      if (defaultSearchTimeoutSec != null) {
        liveSettingsBuilder.setDefaultSearchTimeoutSec(
            DoubleValue.newBuilder().setValue(defaultSearchTimeoutSec).build());
      }
      if (defaultSearchTimeoutCheckEvery != null) {
        liveSettingsBuilder.setDefaultSearchTimeoutCheckEvery(
            Int32Value.newBuilder().setValue(defaultSearchTimeoutCheckEvery).build());
      }
      if (defaultTerminateAfter != null) {
        liveSettingsBuilder.setDefaultTerminateAfter(
            Int32Value.newBuilder().setValue(defaultTerminateAfter).build());
      }
      if (maxMergePreCopyDurationSec != null) {
        liveSettingsBuilder.setMaxMergePreCopyDurationSec(
            UInt64Value.newBuilder().setValue(maxMergePreCopyDurationSec));
      }

      IndexLiveSettings indexLiveSettings = liveSettingsBuilder.build();
      if (!indexLiveSettings.getAllFields().isEmpty()) {
        settingsRequestV2Builder.setLiveSettings(indexLiveSettings);
      }
      client.liveSettingsV2(settingsRequestV2Builder.build());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
