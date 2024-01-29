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

import com.google.gson.JsonParser;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.SettingsRequest;
import com.yelp.nrtsearch.server.grpc.SettingsResponse;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettingsHandler implements Handler<SettingsRequest, SettingsResponse> {
  Logger logger = LoggerFactory.getLogger(SettingsHandler.class);
  private final JsonParser jsonParser = new JsonParser();

  @Override
  public SettingsResponse handle(final IndexState indexStateIn, SettingsRequest settingsRequest)
      throws SettingsHandlerException {
    return handleAsSettingsV2(indexStateIn, settingsRequest);
  }

  private SettingsResponse handleAsSettingsV2(
      IndexState indexState, SettingsRequest settingsRequest) throws SettingsHandlerException {
    IndexStateManager indexStateManager;
    try {
      indexStateManager = indexState.getGlobalState().getIndexStateManager(indexState.getName());
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to get index state manager", e);
    }

    IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();

    if (!settingsRequest.getDirectory().isEmpty()) {
      indexSettingsBuilder.setDirectory(
          StringValue.newBuilder().setValue(settingsRequest.getDirectory()).build());
    }
    if (settingsRequest.getConcurrentMergeSchedulerMaxThreadCount() != 0) {
      indexSettingsBuilder.setConcurrentMergeSchedulerMaxThreadCount(
          Int32Value.newBuilder()
              .setValue(settingsRequest.getConcurrentMergeSchedulerMaxThreadCount())
              .build());
    }
    if (settingsRequest.getConcurrentMergeSchedulerMaxMergeCount() != 0) {
      indexSettingsBuilder.setConcurrentMergeSchedulerMaxMergeCount(
          Int32Value.newBuilder()
              .setValue(settingsRequest.getConcurrentMergeSchedulerMaxMergeCount())
              .build());
    }
    if (settingsRequest.hasIndexSort()) {
      indexSettingsBuilder.setIndexSort(settingsRequest.getIndexSort());
    }
    indexSettingsBuilder.setIndexMergeSchedulerAutoThrottle(
        BoolValue.newBuilder()
            .setValue(settingsRequest.getIndexMergeSchedulerAutoThrottle())
            .build());
    indexSettingsBuilder.setNrtCachingDirectoryMaxSizeMB(
        DoubleValue.newBuilder()
            .setValue(settingsRequest.getNrtCachingDirectoryMaxSizeMB())
            .build());
    indexSettingsBuilder.setNrtCachingDirectoryMaxMergeSizeMB(
        DoubleValue.newBuilder()
            .setValue(settingsRequest.getNrtCachingDirectoryMaxMergeSizeMB())
            .build());

    IndexSettings updatedSettings;
    try {
      updatedSettings = indexStateManager.updateSettings(indexSettingsBuilder.build());
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to update index settings", e);
    }

    String settingsStr;
    try {
      settingsStr = JsonFormat.printer().print(updatedSettings);
    } catch (IOException e) {
      throw new SettingsHandlerException("Unable to print updated settings to json", e);
    }
    return SettingsResponse.newBuilder().setResponse(settingsStr).build();
  }

  public static class SettingsHandlerException extends HandlerException {
    public SettingsHandlerException(String errorMessage) {
      super(errorMessage);
    }

    public SettingsHandlerException(String errorMessage, Throwable err) {
      super(errorMessage, err);
    }
  }
}
