/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.IndexName;
import com.yelp.nrtsearch.server.grpc.SearcherVersion;
import java.io.IOException;

public class ReplicaCurrentSearchingVersionHandler implements Handler<IndexName, SearcherVersion> {

  @Override
  public SearcherVersion handle(IndexState indexState, IndexName indexNameRequest)
      throws HandlerException {
    ShardState shardState = indexState.getShard(0);
    if (!shardState.isReplica() || !shardState.isStarted()) {
      throw new IllegalArgumentException(
          "index \""
              + indexNameRequest.getIndexName()
              + "\" is not a replica or was not started yet");
    }
    try {
      long currentSearchingVersion = shardState.nrtReplicaNode.getCurrentSearchingVersion();
      return SearcherVersion.newBuilder().setVersion(currentSearchingVersion).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
