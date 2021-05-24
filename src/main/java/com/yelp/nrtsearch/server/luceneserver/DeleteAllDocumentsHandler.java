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

import com.yelp.nrtsearch.server.grpc.*;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteAllDocumentsHandler
    implements Handler<DeleteAllDocumentsRequest, DeleteAllDocumentsResponse> {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteAllDocumentsHandler.class.getName());

  @Override
  public DeleteAllDocumentsResponse handle(
      IndexState indexState, DeleteAllDocumentsRequest deleteAllDocumentsRequest)
      throws DeleteAllDocumentsHandlerException {
    final ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();
    long gen;
    try {
      gen = shardState.writer.deleteAll();
    } catch (IOException e) {
      logger.warn(
          "ThreadId: {}, writer.deleteAll failed",
          Thread.currentThread().getName() + Thread.currentThread().getId());
      throw new DeleteAllDocumentsHandlerException(e);
    }
    return DeleteAllDocumentsResponse.newBuilder().setGenId(String.valueOf(gen)).build();
  }

  public static class DeleteAllDocumentsHandlerException extends Handler.HandlerException {

    public DeleteAllDocumentsHandlerException(Throwable err) {
      super(err);
    }
  }
}
