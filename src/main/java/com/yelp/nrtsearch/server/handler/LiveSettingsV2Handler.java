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
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.LiveSettingsV2Request;
import com.yelp.nrtsearch.server.grpc.LiveSettingsV2Response;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static helper class to handle a LiveSettingsV2 request and produce a response. */
public class LiveSettingsV2Handler extends Handler<LiveSettingsV2Request, LiveSettingsV2Response> {
  private static final Logger logger = LoggerFactory.getLogger(LiveSettingsV2Handler.class);

  public LiveSettingsV2Handler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public LiveSettingsV2Response handle(LiveSettingsV2Request req) throws Exception {
    logger.info("Received live settings V2 request: {}", req);
    IndexStateManager indexStateManager = getIndexStateManager(req.getIndexName());
    LiveSettingsV2Response reply = handle(indexStateManager, req);
    logger.info("LiveSettingsV2Handler returned " + JsonFormat.printer().print(reply));
    return reply;
  }

  /**
   * Handle a LiveSettingsV2 request.
   *
   * @param indexStateManager state manager for index
   * @param liveSettingsRequest request message
   * @return response message
   * @throws IOException on error committing state
   */
  public static LiveSettingsV2Response handle(
      IndexStateManager indexStateManager, LiveSettingsV2Request liveSettingsRequest)
      throws IOException {
    IndexLiveSettings responseSettings;
    // if there is no settings message in the request, just return the current settings
    if (liveSettingsRequest.hasLiveSettings()) {
      responseSettings =
          indexStateManager.updateLiveSettings(
              liveSettingsRequest.getLiveSettings(), liveSettingsRequest.getLocal());
    } else {
      responseSettings = indexStateManager.getLiveSettings(liveSettingsRequest.getLocal());
    }
    return LiveSettingsV2Response.newBuilder().setLiveSettings(responseSettings).build();
  }
}
