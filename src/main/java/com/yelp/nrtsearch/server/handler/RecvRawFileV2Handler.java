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
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecvRawFileV2Handler extends Handler<FileInfo, RawFileChunk> {
  private static final Logger logger = LoggerFactory.getLogger(RecvRawFileV2Handler.class);
  private final boolean verifyIndexId;

  public RecvRawFileV2Handler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public StreamObserver<FileInfo> handle(StreamObserver<RawFileChunk> responseObserver) {
    return new StreamObserver<>() {
      private IndexState indexState;
      private IndexInput luceneFile;
      private byte[] buffer;
      private final int ackEvery =
          getGlobalState().getConfiguration().getFileCopyConfig().getAckEvery();
      private final int maxInflight =
          getGlobalState().getConfiguration().getFileCopyConfig().getMaxInFlight();
      private int lastAckedSeq = 0;
      private int currentSeq = 0;
      private long fileOffset;
      private long fileLength;

      @Override
      public void onNext(FileInfo fileInfoRequest) {
        try {
          if (indexState == null) {
            // Start transfer
            IndexStateManager indexStateManager =
                getGlobalState().getIndexStateManagerOrThrow(fileInfoRequest.getIndexName());
            checkIndexId(
                fileInfoRequest.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

            indexState = indexStateManager.getCurrent();
            ShardState shardState = indexState.getShard(0);
            if (shardState == null) {
              throw new IllegalStateException(
                  "Error getting shard state for: " + fileInfoRequest.getIndexName());
            }
            luceneFile =
                shardState.indexDir.openInput(fileInfoRequest.getFileName(), IOContext.DEFAULT);
            luceneFile.seek(fileInfoRequest.getFpStart());
            fileOffset = fileInfoRequest.getFpStart();
            fileLength = luceneFile.length();
            buffer =
                new byte[getGlobalState().getConfiguration().getFileCopyConfig().getChunkSize()];
          } else {
            // ack existing transfer
            lastAckedSeq = fileInfoRequest.getAckSeqNum();
            if (lastAckedSeq <= 0) {
              throw new IllegalArgumentException(
                  "Invalid ackSeqNum: " + fileInfoRequest.getAckSeqNum());
            }
          }
          while (fileOffset < fileLength && (currentSeq - lastAckedSeq) < maxInflight) {
            int chunkSize = (int) Math.min(buffer.length, (fileLength - fileOffset));
            luceneFile.readBytes(buffer, 0, chunkSize);
            currentSeq++;
            RawFileChunk rawFileChunk =
                RawFileChunk.newBuilder()
                    .setContent(ByteString.copyFrom(buffer, 0, chunkSize))
                    .setSeqNum(currentSeq)
                    .setAck((currentSeq % ackEvery) == 0)
                    .build();
            responseObserver.onNext(rawFileChunk);
            fileOffset += chunkSize;
            if (fileOffset == fileLength) {
              responseObserver.onCompleted();
            }
          }
          logger.debug(
              String.format("recvRawFileV2: in flight chunks: %d", currentSeq - lastAckedSeq));
        } catch (Throwable t) {
          maybeCloseFile();
          responseObserver.onError(t);
          throw new RuntimeException(t);
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.error("recvRawFileV2 onError", t);
        maybeCloseFile();
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        maybeCloseFile();
        logger.debug("recvRawFileV2 onCompleted");
      }

      private void maybeCloseFile() {
        if (luceneFile != null) {
          try {
            luceneFile.close();
          } catch (IOException e) {
            logger.warn("Error closing index file", e);
          }
          luceneFile = null;
        }
      }
    };
  }
}
