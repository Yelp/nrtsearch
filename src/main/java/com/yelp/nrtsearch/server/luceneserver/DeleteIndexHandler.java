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

import com.yelp.nrtsearch.server.grpc.DeleteIndexRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexResponse;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteIndexHandler implements Handler<DeleteIndexRequest, DeleteIndexResponse> {
  private static final Logger logger = Logger.getLogger(DeleteIndexHandler.class.getName());

  @Override
  public DeleteIndexResponse handle(IndexState indexState, DeleteIndexRequest protoRequest)
      throws DeleteIndexHandlerException {
    try {
      indexState.close();
      indexState.deleteIndex();
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "ThreadId: %s, deleteIndex failed",
              Thread.currentThread().getName() + Thread.currentThread().getId()));
      throw new DeleteIndexHandlerException(e);
    }
    return DeleteIndexResponse.newBuilder().setOk("ok").build();
  }

  public static class DeleteIndexHandlerException extends Handler.HandlerException {

    public DeleteIndexHandlerException(Throwable err) {
      super(err);
    }
  }
}
