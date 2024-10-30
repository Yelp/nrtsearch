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

import com.yelp.nrtsearch.server.grpc.IndexStateRequest;
import com.yelp.nrtsearch.server.grpc.IndexStateResponse;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.state.GlobalState;

public class IndexStateHandler extends Handler<IndexStateRequest, IndexStateResponse> {
  public IndexStateHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public IndexStateResponse handle(IndexStateRequest request) throws Exception {
    IndexState indexState = getIndexState(request.getIndexName());

    return IndexStateResponse.newBuilder().setIndexState(indexState.getIndexStateInfo()).build();
  }
}
