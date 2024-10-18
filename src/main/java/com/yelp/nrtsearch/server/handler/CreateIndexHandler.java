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

import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexResponse;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexHandler extends Handler<CreateIndexRequest, CreateIndexResponse> {
  private static final Logger logger = LoggerFactory.getLogger(CreateIndexHandler.class);

  public CreateIndexHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public CreateIndexResponse handle(CreateIndexRequest req) throws Exception {
    logger.info("Received create index request: {}", req);
    String indexName = req.getIndexName();
    String validIndexNameRegex = "[A-z0-9_-]+";
    if (!indexName.matches(validIndexNameRegex)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Index name %s is invalid - must contain only a-z, A-Z or 0-9", indexName))
          .asRuntimeException();
    }

    synchronized (getGlobalState()) {
      if (getGlobalState().getIndex(indexName) != null) {
        throw Status.ALREADY_EXISTS
            .withDescription(String.format("Index %s already exists", indexName))
            .asRuntimeException();
      }
      getGlobalState().createIndex(req);
    }
    String response = String.format("Created Index name: %s", indexName);
    return CreateIndexResponse.newBuilder().setResponse(response).build();
  }
}
