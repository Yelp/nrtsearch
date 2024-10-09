/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler;

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.SettingsV2Request;
import com.yelp.nrtsearch.server.grpc.SettingsV2Response;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static helper class to handle a SettingsV2 request and produce a response. */
public class SettingsV2Handler extends Handler<SettingsV2Request, SettingsV2Response> {
  private static final Logger logger = LoggerFactory.getLogger(SettingsV2Handler.class);

  public SettingsV2Handler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      SettingsV2Request settingsRequest, StreamObserver<SettingsV2Response> responseObserver) {
    logger.info("Received settings V2 request: {}", settingsRequest);
    try {
      IndexStateManager indexStateManager =
          getGlobalState().getIndexStateManagerOrThrow(settingsRequest.getIndexName());
      SettingsV2Response reply = handle(indexStateManager, settingsRequest);
      logger.info("SettingsV2Handler returned: " + JsonFormat.printer().print(reply));
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (IOException e) {
      logger.warn(
          "error while trying to read index state dir for indexName: "
              + settingsRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "error while trying to read index state dir for indexName: "
                      + settingsRequest.getIndexName())
              .augmentDescription("IOException()")
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      logger.warn(
          "error while trying to update/get settings for index " + settingsRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "error while trying to update/get settings for index: "
                      + settingsRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  /**
   * Handle a SettingsV2 request.
   *
   * @param indexStateManager state manager for index
   * @param settingsRequest request message
   * @return response message
   * @throws IOException on error committing state
   */
  public static SettingsV2Response handle(
      final IndexStateManager indexStateManager, SettingsV2Request settingsRequest)
      throws IOException {
    IndexSettings responseSettings;
    // if there is no settings message in the request, just return the current settings
    if (settingsRequest.hasSettings()) {
      responseSettings = indexStateManager.updateSettings(settingsRequest.getSettings());
    } else {
      responseSettings = indexStateManager.getSettings();
    }
    return SettingsV2Response.newBuilder().setSettings(responseSettings).build();
  }
}
