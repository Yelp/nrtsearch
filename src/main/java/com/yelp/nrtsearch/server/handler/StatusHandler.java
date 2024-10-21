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

import com.yelp.nrtsearch.server.grpc.HealthCheckRequest;
import com.yelp.nrtsearch.server.grpc.HealthCheckResponse;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusHandler extends Handler<HealthCheckRequest, HealthCheckResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StatusHandler.class);

  public StatusHandler() {
    super(null);
  }

  @Override
  public HealthCheckResponse handle(HealthCheckRequest request) throws Exception {
    HealthCheckResponse reply =
        HealthCheckResponse.newBuilder().setHealth(TransferStatusCode.Done).build();
    logger.debug("HealthCheckResponse returned {}", reply);
    return reply;
  }
}
