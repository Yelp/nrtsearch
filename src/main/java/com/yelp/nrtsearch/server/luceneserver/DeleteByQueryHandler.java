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

import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.grpc.DeleteByQueryRequest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteByQueryHandler implements Handler<DeleteByQueryRequest, AddDocumentResponse> {
  private static final Logger logger = LoggerFactory.getLogger(DeleteByQueryHandler.class);
  private final QueryNodeMapper queryNodeMapper = QueryNodeMapper.getInstance();

  @Override
  public AddDocumentResponse handle(
      IndexState indexState, DeleteByQueryRequest deleteByQueryRequest)
      throws DeleteByQueryHandlerException {
    final ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();

    List<Query> queryList =
        deleteByQueryRequest.getQueryList().stream()
            .map(query -> queryNodeMapper.getQuery(query, indexState))
            .collect(Collectors.toList());
    try {
      shardState.writer.deleteDocuments(queryList.toArray(new Query[] {}));
    } catch (IOException e) {
      logger.warn(
          "ThreadId: {}, writer.deleteDocuments failed",
          Thread.currentThread().getName() + Thread.currentThread().getId());
      throw new DeleteByQueryHandlerException(e);
    }
    long genId = shardState.writer.getMaxCompletedSequenceNumber();
    return AddDocumentResponse.newBuilder()
        .setGenId(String.valueOf(genId))
        .setPrimaryId(indexState.getGlobalState().getEphemeralId())
        .build();
  }

  public static class DeleteByQueryHandlerException extends Handler.HandlerException {

    public DeleteByQueryHandlerException(Throwable err) {
      super(err);
    }
  }
}
