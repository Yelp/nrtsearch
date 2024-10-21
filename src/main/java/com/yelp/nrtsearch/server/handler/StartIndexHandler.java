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

import com.yelp.nrtsearch.server.grpc.StartIndexRequest;
import com.yelp.nrtsearch.server.grpc.StartIndexResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartIndexHandler extends Handler<StartIndexRequest, StartIndexResponse> {
  private static final Logger logger = LoggerFactory.getLogger(StartIndexHandler.class);

  public StartIndexHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public StartIndexResponse handle(StartIndexRequest startIndexRequest) throws Exception {
    logger.info("Received start index request: {}", startIndexRequest);
    if (startIndexRequest.getIndexName().isEmpty()) {
      logger.warn("error while trying to start index with empty index name.");
      throw Status.INVALID_ARGUMENT
          .withDescription("error while trying to start index since indexName was empty.")
          .asRuntimeException();
    }

    StartIndexResponse reply = getGlobalState().startIndex(startIndexRequest);
    logger.info("StartIndexHandler returned {}", reply.toString());
    return reply;
  }
}
