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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.yelp.nrtsearch.server.grpc.HealthCheckResponse;
import com.yelp.nrtsearch.server.grpc.ReadyCheckRequest;
import com.yelp.nrtsearch.server.grpc.TransferStatusCode;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadyHandler extends Handler<ReadyCheckRequest, HealthCheckResponse> {
  private static final Logger logger = LoggerFactory.getLogger(ReadyHandler.class);
  private static final Splitter COMMA_SPLITTER = Splitter.on(",");

  public ReadyHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public HealthCheckResponse handle(ReadyCheckRequest request) throws Exception {
    Set<String> indexNames;

    // If specific index names are provided we will check only those indices, otherwise check all
    if (request.getIndexNames().isEmpty()) {
      indexNames = getGlobalState().getIndicesToStart();
    } else {
      List<String> indexNamesToCheck = COMMA_SPLITTER.splitToList(request.getIndexNames());

      Set<String> allIndices = getGlobalState().getIndexNames();

      Sets.SetView<String> nonExistentIndices =
          Sets.difference(Set.copyOf(indexNamesToCheck), allIndices);
      if (!nonExistentIndices.isEmpty()) {
        logger.warn("Indices: {} do not exist", nonExistentIndices);
        throw Status.UNAVAILABLE
            .withDescription(String.format("Indices do not exist: %s", nonExistentIndices))
            .asRuntimeException();
      }

      indexNames =
          allIndices.stream().filter(indexNamesToCheck::contains).collect(Collectors.toSet());
    }

    List<String> indicesNotStarted = new ArrayList<>();
    for (String indexName : indexNames) {
      IndexState indexState = getGlobalState().getIndexOrThrow(indexName);
      if (!indexState.isStarted()) {
        indicesNotStarted.add(indexName);
      }
    }

    if (indicesNotStarted.isEmpty()) {
      HealthCheckResponse reply =
          HealthCheckResponse.newBuilder().setHealth(TransferStatusCode.Done).build();
      logger.debug("Ready check returned " + reply);
      return reply;
    } else {
      logger.warn("Indices not started: {}", indicesNotStarted);
      throw Status.UNAVAILABLE
          .withDescription(String.format("Indices not started: %s", indicesNotStarted))
          .asRuntimeException();
    }
  }
}
