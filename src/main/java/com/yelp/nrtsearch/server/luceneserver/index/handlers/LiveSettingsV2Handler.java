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
package com.yelp.nrtsearch.server.luceneserver.index.handlers;

import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.LiveSettingsV2Request;
import com.yelp.nrtsearch.server.grpc.LiveSettingsV2Response;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import java.io.IOException;

/** Static helper class to handle a LiveSettingsV2 request and produce a response. */
public class LiveSettingsV2Handler {

  private LiveSettingsV2Handler() {}

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
          indexStateManager.updateLiveSettings(liveSettingsRequest.getLiveSettings());
    } else {
      responseSettings = indexStateManager.getLiveSettings();
    }
    return LiveSettingsV2Response.newBuilder().setLiveSettings(responseSettings).build();
  }
}
