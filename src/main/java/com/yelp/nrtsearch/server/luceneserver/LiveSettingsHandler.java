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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveSettingsHandler implements Handler<LiveSettingsRequest, LiveSettingsResponse> {
  Logger logger = LoggerFactory.getLogger(LiveSettingsHandler.class);

  @Override
  public LiveSettingsResponse handle(
      IndexState indexState, LiveSettingsRequest liveSettingsRequest) {
    logger.info(
        String.format("update liveSettings for index:  %s", liveSettingsRequest.getIndexName()));
    if (liveSettingsRequest.getMaxRefreshSec() != 0) {
      indexState.setMaxRefreshSec(liveSettingsRequest.getMaxRefreshSec());
      logger.info(String.format("set maxRefreshSec: %s", liveSettingsRequest.getMaxRefreshSec()));
    }
    if (liveSettingsRequest.getMinRefreshSec() != 0) {
      indexState.setMinRefreshSec(liveSettingsRequest.getMinRefreshSec());
      logger.info(String.format("set minRefreshSec: %s", liveSettingsRequest.getMinRefreshSec()));
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
    String response = indexState.getLiveSettingsJSON();
    LiveSettingsResponse reply = LiveSettingsResponse.newBuilder().setResponse(response).build();
    return reply;
  }
}
