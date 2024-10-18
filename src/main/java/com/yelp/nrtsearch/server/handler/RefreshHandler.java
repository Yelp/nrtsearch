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

import com.yelp.nrtsearch.server.grpc.RefreshRequest;
import com.yelp.nrtsearch.server.grpc.RefreshResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshHandler extends Handler<RefreshRequest, RefreshResponse> {
  private static final Logger logger = LoggerFactory.getLogger(RefreshHandler.class);

  public RefreshHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public RefreshResponse handle(RefreshRequest refreshRequest) throws Exception {
    IndexState indexState = getIndexState(refreshRequest.getIndexName());
    final ShardState shardState = indexState.getShard(0);
    long t0 = System.nanoTime();
    shardState.maybeRefreshBlocking();
    long t1 = System.nanoTime();
    double refreshTimeMs = (t1 - t0) / 1000000.0;
    RefreshResponse reply = RefreshResponse.newBuilder().setRefreshTimeMS(refreshTimeMs).build();
    logger.info(
        String.format(
            "RefreshHandler refreshed index: %s in %f",
            refreshRequest.getIndexName(), refreshTimeMs));
    return reply;
  }
}
