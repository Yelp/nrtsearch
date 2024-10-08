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

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.FileInfo;
import com.yelp.nrtsearch.server.grpc.RawFileChunk;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecvRawFileHandler extends Handler<FileInfo, RawFileChunk> {
  private static final Logger logger = LoggerFactory.getLogger(RecvRawFileHandler.class);
  private final boolean verifyIndexId;

  public RecvRawFileHandler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public void handle(FileInfo fileInfoRequest, StreamObserver<RawFileChunk> responseObserver) {
    try {
      IndexStateManager indexStateManager =
          getGlobalState().getIndexStateManagerOrThrow(fileInfoRequest.getIndexName());
      checkIndexId(fileInfoRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

      IndexState indexState = indexStateManager.getCurrent();
      ShardState shardState = indexState.getShard(0);
      try (IndexInput luceneFile =
          shardState.indexDir.openInput(fileInfoRequest.getFileName(), IOContext.DEFAULT)) {
        long len = luceneFile.length();
        long pos = fileInfoRequest.getFpStart();
        luceneFile.seek(pos);
        byte[] buffer = new byte[1024 * 64];
        long totalRead;
        totalRead = pos;
        while (totalRead < len) {
          int chunkSize = (int) Math.min(buffer.length, (len - totalRead));
          luceneFile.readBytes(buffer, 0, chunkSize);
          RawFileChunk rawFileChunk =
              RawFileChunk.newBuilder()
                  .setContent(ByteString.copyFrom(buffer, 0, chunkSize))
                  .build();
          responseObserver.onNext(rawFileChunk);
          totalRead += chunkSize;
        }
        // EOF
        responseObserver.onCompleted();
      }
    } catch (StatusRuntimeException e) {
      logger.warn("error on recvRawFile " + fileInfoRequest.getFileName(), e);
      responseObserver.onError(e);
    } catch (Exception e) {
      logger.warn("error on recvRawFile " + fileInfoRequest.getFileName(), e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("error on recvRawFile: " + fileInfoRequest.getFileName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }
}
