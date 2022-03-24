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

import com.yelp.nrtsearch.server.grpc.LiveSettingsRequest;
import com.yelp.nrtsearch.server.grpc.LiveSettingsResponse;
import com.yelp.nrtsearch.server.luceneserver.index.LegacyIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveSettingsHandler implements Handler<LiveSettingsRequest, LiveSettingsResponse> {
  private static final Logger logger = LoggerFactory.getLogger(LiveSettingsHandler.class);

  @Override
  public LiveSettingsResponse handle(
      IndexState indexStateIn, LiveSettingsRequest liveSettingsRequest) {
    if (!(indexStateIn instanceof LegacyIndexState)) {
      throw new IllegalArgumentException("Only LegacyIndexState is supported");
    }
    LegacyIndexState indexState = (LegacyIndexState) indexStateIn;
    logger.info(
        String.format("update liveSettings for index:  %s", liveSettingsRequest.getIndexName()));
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
      indexState.setRefreshSec(minSec, maxSec);
      logger.info(String.format("set minRefreshSec: %s, maxRefreshSec: %s", minSec, maxSec));
    }
    if (liveSettingsRequest.getMaxSearcherAgeSec() != 0) {
      indexState.setMaxSearcherAgeSec(liveSettingsRequest.getMaxSearcherAgeSec());
      logger.info(
          String.format("set maxSearcherAgeSec: %s", liveSettingsRequest.getMaxSearcherAgeSec()));
    }
    if (liveSettingsRequest.getIndexRamBufferSizeMB() != 0) {
      indexState.setIndexRamBufferSizeMB(liveSettingsRequest.getIndexRamBufferSizeMB());
      logger.info(
          String.format(
              "set indexRamBufferSizeMB: %s", liveSettingsRequest.getIndexRamBufferSizeMB()));
    }
    if (liveSettingsRequest.getAddDocumentsMaxBufferLen() != 0) {
      indexState.setAddDocumentsMaxBufferLen(liveSettingsRequest.getAddDocumentsMaxBufferLen());
      logger.info(
          String.format(
              "set addDocumentsMaxBufferLen: %s",
              liveSettingsRequest.getAddDocumentsMaxBufferLen()));
    }
    if (liveSettingsRequest.getSliceMaxDocs() != 0) {
      indexState.setSliceMaxDocs(liveSettingsRequest.getSliceMaxDocs());
      logger.info(String.format("set sliceMaxDocs: %s", liveSettingsRequest.getSliceMaxDocs()));
    }
    if (liveSettingsRequest.getSliceMaxSegments() != 0) {
      indexState.setSliceMaxSegments(liveSettingsRequest.getSliceMaxSegments());
      logger.info(
          String.format("set sliceMaxSegments: %s", liveSettingsRequest.getSliceMaxSegments()));
    }
    if (liveSettingsRequest.getVirtualShards() != 0) {
      indexState.setVirtualShards(liveSettingsRequest.getVirtualShards());
      logger.info(String.format("set virtualShards: %s", liveSettingsRequest.getVirtualShards()));
    }
    if (liveSettingsRequest.getMaxMergedSegmentMB() != 0) {
      indexState.setMaxMergedSegmentMB(liveSettingsRequest.getMaxMergedSegmentMB());
      logger.info(
          String.format("set maxMergedSegmentMB: %s", liveSettingsRequest.getMaxMergedSegmentMB()));
    }
    if (liveSettingsRequest.getSegmentsPerTier() != 0) {
      indexState.setSegmentsPerTier(liveSettingsRequest.getSegmentsPerTier());
      logger.info(
          String.format("set segmentsPerTier: %s", liveSettingsRequest.getSegmentsPerTier()));
    }
    if (liveSettingsRequest.getDefaultSearchTimeoutSec() >= 0) {
      indexState.setDefaultSearchTimeoutSec(liveSettingsRequest.getDefaultSearchTimeoutSec());
      logger.info(
          String.format(
              "set defaultSearchTimeoutSec: %s", liveSettingsRequest.getDefaultSearchTimeoutSec()));
    }
    if (liveSettingsRequest.getDefaultSearchTimeoutCheckEvery() >= 0) {
      indexState.setDefaultSearchTimeoutCheckEvery(
          liveSettingsRequest.getDefaultSearchTimeoutCheckEvery());
      logger.info(
          String.format(
              "set defaultSearchTimeoutCheckEvery: %s",
              liveSettingsRequest.getDefaultSearchTimeoutCheckEvery()));
    }
    if (liveSettingsRequest.getDefaultTerminateAfter() >= 0) {
      indexState.setDefaultTerminateAfter(liveSettingsRequest.getDefaultTerminateAfter());
      logger.info(
          String.format(
              "set defaultTerminateAfter: %s", liveSettingsRequest.getDefaultTerminateAfter()));
    }
    String response = indexState.getLiveSettingsJSON();
    return LiveSettingsResponse.newBuilder().setResponse(response).build();
  }
}
