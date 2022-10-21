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
package com.yelp.nrtsearch.server.luceneserver;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsResponse;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveSettingsHandler implements Handler<LiveSettingsRequest, LiveSettingsResponse> {
  private static final Logger logger = LoggerFactory.getLogger(LiveSettingsHandler.class);

  @Override
  public LiveSettingsResponse handle(
      IndexState indexStateIn, LiveSettingsRequest liveSettingsRequest) {
    return handleAsLiveSettingsV2(indexStateIn, liveSettingsRequest);
  }

  private LiveSettingsResponse handleAsLiveSettingsV2(
      IndexState indexStateIn, LiveSettingsRequest liveSettingsRequest) {
    IndexStateManager indexStateManager;
    try {
      indexStateManager =
          indexStateIn.getGlobalState().getIndexStateManager(indexStateIn.getName());
    } catch (IOException e) {
      throw new RuntimeException("Unable to get index state manager", e);
    }

    IndexLiveSettings updatedSettings;
    // synchronize on the state manager so no state changes can be made while reading
    // values from the current state
    synchronized (indexStateManager) {
      IndexState indexState = indexStateManager.getCurrent();
      IndexLiveSettings.Builder settingsBuilder = IndexLiveSettings.newBuilder();

      if (liveSettingsRequest.getMaxRefreshSec() != 0
          || liveSettingsRequest.getMinRefreshSec() != 0) {
        double maxSec =
            liveSettingsRequest.getMaxRefreshSec() != 0
                ? liveSettingsRequest.getMaxRefreshSec()
                : indexState.getMaxRefreshSec();
        double minSec =
            liveSettingsRequest.getMinRefreshSec() != 0
                ? liveSettingsRequest.getMinRefreshSec()
                : indexState.getMinRefreshSec();
        settingsBuilder.setMaxRefreshSec(DoubleValue.newBuilder().setValue(maxSec).build());
        settingsBuilder.setMinRefreshSec(DoubleValue.newBuilder().setValue(minSec).build());
      }
      if (liveSettingsRequest.getMaxSearcherAgeSec() != 0) {
        settingsBuilder.setMaxSearcherAgeSec(
            DoubleValue.newBuilder().setValue(liveSettingsRequest.getMaxSearcherAgeSec()).build());
      }
      if (liveSettingsRequest.getIndexRamBufferSizeMB() != 0) {
        settingsBuilder.setIndexRamBufferSizeMB(
            DoubleValue.newBuilder()
                .setValue(liveSettingsRequest.getIndexRamBufferSizeMB())
                .build());
      }
      if (liveSettingsRequest.getAddDocumentsMaxBufferLen() != 0) {
        settingsBuilder.setAddDocumentsMaxBufferLen(
            Int32Value.newBuilder()
                .setValue(liveSettingsRequest.getAddDocumentsMaxBufferLen())
                .build());
      }
      if (liveSettingsRequest.getSliceMaxDocs() != 0) {
        settingsBuilder.setSliceMaxDocs(
            Int32Value.newBuilder().setValue(liveSettingsRequest.getSliceMaxDocs()).build());
      }
      if (liveSettingsRequest.getSliceMaxSegments() != 0) {
        settingsBuilder.setSliceMaxSegments(
            Int32Value.newBuilder().setValue(liveSettingsRequest.getSliceMaxSegments()).build());
      }
      if (liveSettingsRequest.getVirtualShards() != 0) {
        settingsBuilder.setVirtualShards(
            Int32Value.newBuilder().setValue(liveSettingsRequest.getVirtualShards()).build());
      }
      if (liveSettingsRequest.getMaxMergedSegmentMB() != 0) {
        settingsBuilder.setMaxMergedSegmentMB(
            Int32Value.newBuilder().setValue(liveSettingsRequest.getMaxMergedSegmentMB()).build());
      }
      if (liveSettingsRequest.getSegmentsPerTier() != 0) {
        settingsBuilder.setSegmentsPerTier(
            Int32Value.newBuilder().setValue(liveSettingsRequest.getSegmentsPerTier()).build());
      }
      if (liveSettingsRequest.getDefaultSearchTimeoutSec() >= 0) {
        settingsBuilder.setDefaultSearchTimeoutSec(
            DoubleValue.newBuilder()
                .setValue(liveSettingsRequest.getDefaultSearchTimeoutSec())
                .build());
      }
      if (liveSettingsRequest.getDefaultSearchTimeoutCheckEvery() >= 0) {
        settingsBuilder.setDefaultSearchTimeoutCheckEvery(
            Int32Value.newBuilder()
                .setValue(liveSettingsRequest.getDefaultSearchTimeoutCheckEvery())
                .build());
      }
      if (liveSettingsRequest.getDefaultTerminateAfter() >= 0) {
        settingsBuilder.setDefaultTerminateAfter(
            Int32Value.newBuilder()
                .setValue(liveSettingsRequest.getDefaultTerminateAfter())
                .build());
      }
      try {
        updatedSettings = indexStateManager.updateLiveSettings(settingsBuilder.build());
      } catch (IOException e) {
        throw new RuntimeException("Unable to update index live settings", e);
      }
    }
    String settingsStr;
    try {
      settingsStr = JsonFormat.printer().print(updatedSettings);
    } catch (IOException e) {
      throw new RuntimeException("Unable to print updated settings to json", e);
    }
    return LiveSettingsResponse.newBuilder().setResponse(settingsStr).build();
  }
}
