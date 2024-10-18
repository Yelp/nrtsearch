/*
 * Copyright 2024 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReloadStateRequest;
import com.yelp.nrtsearch.server.grpc.ReloadStateResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadStateHandler extends Handler<ReloadStateRequest, ReloadStateResponse> {
  private static final Logger logger = LoggerFactory.getLogger(ReloadStateHandler.class);

  public ReloadStateHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public ReloadStateResponse handle(ReloadStateRequest request) throws Exception {
    if (getGlobalState().getConfiguration().getIndexStartConfig().getMode().equals(Mode.REPLICA)) {
      getGlobalState().reloadStateFromBackend();
    } else {
      logger.info("Skip reloading state since it is not replica");
    }
    return ReloadStateResponse.newBuilder().build();
  }
}
