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

import com.yelp.nrtsearch.server.grpc.IndexSettings;
import com.yelp.nrtsearch.server.grpc.SettingsV2Request;
import com.yelp.nrtsearch.server.grpc.SettingsV2Response;
import com.yelp.nrtsearch.server.luceneserver.index.IndexStateManager;
import java.io.IOException;

/** Static helper class to handle a SettingsV2 request and produce a response. */
public class SettingsV2Handler {

  private SettingsV2Handler() {}

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
