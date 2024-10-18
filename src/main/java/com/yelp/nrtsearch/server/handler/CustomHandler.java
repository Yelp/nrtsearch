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

import com.yelp.nrtsearch.server.custom.request.CustomRequestProcessor;
import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHandler extends Handler<CustomRequest, CustomResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CustomHandler.class);

  public CustomHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public CustomResponse handle(CustomRequest request) throws Exception {
    logger.info("Received custom request: {}", request);
    return CustomRequestProcessor.processCustomRequest(request);
  }
}
