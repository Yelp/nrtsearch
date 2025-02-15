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

import com.yelp.nrtsearch.server.grpc.DummyResponse;
import com.yelp.nrtsearch.server.grpc.StopIndexRequest;
import com.yelp.nrtsearch.server.state.GlobalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopIndexHandler extends Handler<StopIndexRequest, DummyResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StopIndexHandler.class);

  public StopIndexHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public DummyResponse handle(StopIndexRequest stopIndexRequest) throws Exception {
    logger.info("Received stop index request: {}", stopIndexRequest);
    DummyResponse reply = getGlobalState().stopIndex(stopIndexRequest);

    logger.info("StopIndexHandler returned {}", reply);
    return reply;
  }
}
