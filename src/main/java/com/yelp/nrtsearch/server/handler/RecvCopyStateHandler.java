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
package com.yelp.nrtsearch.server.handler;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.CopyState;
import com.yelp.nrtsearch.server.grpc.CopyStateRequest;
import com.yelp.nrtsearch.server.grpc.FileMetadata;
import com.yelp.nrtsearch.server.grpc.FilesMetadata;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.nrt.NRTPrimaryNode;
import com.yelp.nrtsearch.server.state.GlobalState;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecvCopyStateHandler extends Handler<CopyStateRequest, CopyState> {
  private static final Logger logger = LoggerFactory.getLogger(RecvCopyStateHandler.class);

  private final boolean verifyIndexId;

  public RecvCopyStateHandler(GlobalState globalState, boolean verifyIndexId) {
    super(globalState);
    this.verifyIndexId = verifyIndexId;
  }

  @Override
  public CopyState handle(CopyStateRequest request) throws Exception {
    IndexStateManager indexStateManager = getIndexStateManager(request.getIndexName());
    checkIndexId(request.getIndexId(), indexStateManager.getIndexId(), verifyIndexId);

    IndexState indexState = indexStateManager.getCurrent();
    CopyState reply = handle(indexState, request);
    logger.debug(
        "RecvCopyStateHandler returned, completedMergeFiles count: {}",
        reply.getCompletedMergeFilesCount());
    return reply;
  }

  private CopyState handle(IndexState indexState, CopyStateRequest copyStateRequest) {
    ShardState shardState = indexState.getShard(0);
    if (shardState.isPrimary() == false) {
      throw new IllegalArgumentException(
          "index \"" + indexState.getName() + "\" was not started or is not a primary");
    }

    if (!isValidMagicHeader(copyStateRequest.getMagicNumber())) {
      throw new RuntimeException("RecvCopyStateHandler invoked with Invalid Magic Number");
    }
    NRTPrimaryNode primaryNode = shardState.nrtPrimaryNode;
    org.apache.lucene.replicator.nrt.CopyState copyState = null;
    try {
      // Caller does not have CopyState; we pull the latest NRT point:
      copyState = primaryNode.getCopyState();
      return RecvCopyStateHandler.writeCopyState(copyState);
    } catch (IOException e) {
      primaryNode.message("top: exception during fetch: " + e.getMessage());
      throw new RuntimeException(e);
    } finally {
      if (copyState != null) {
        primaryNode.message("top: fetch: now release CopyState");
        try {
          primaryNode.releaseCopyState(copyState);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static CopyState writeCopyState(org.apache.lucene.replicator.nrt.CopyState state)
      throws IOException {
    CopyState.Builder builder = CopyState.newBuilder();
    builder.setInfoBytesLength(state.infosBytes.length);
    builder.setInfoBytes(ByteString.copyFrom(state.infosBytes, 0, state.infosBytes.length));

    builder.setGen(state.gen);
    builder.setVersion(state.version);

    FilesMetadata filesMetadata = writeFilesMetaData(state.files);
    builder.setFilesMetadata(filesMetadata);

    builder.setCompletedMergeFilesSize(state.completedMergeFiles.size());

    for (String fileName : state.completedMergeFiles) {
      builder.addCompletedMergeFiles(fileName);
    }

    builder.setPrimaryGen(state.primaryGen);

    return builder.build();
  }

  public static FilesMetadata writeFilesMetaData(Map<String, FileMetaData> files) {
    FilesMetadata.Builder builder = FilesMetadata.newBuilder();
    builder.setNumFiles(files.size());

    for (Map.Entry<String, FileMetaData> ent : files.entrySet()) {
      FileMetadata.Builder fileMetadataBuilder = FileMetadata.newBuilder();
      fileMetadataBuilder.setFileName(ent.getKey());

      FileMetaData fmd = ent.getValue();
      fileMetadataBuilder.setLen(fmd.length);
      fileMetadataBuilder.setChecksum(fmd.checksum);
      fileMetadataBuilder.setHeaderLength(fmd.header.length);
      fileMetadataBuilder.setHeader(ByteString.copyFrom(fmd.header, 0, fmd.header.length));
      fileMetadataBuilder.setFooterLength(fmd.footer.length);
      fileMetadataBuilder.setFooter(ByteString.copyFrom(fmd.footer, 0, fmd.footer.length));
      builder.addFileMetadata(fileMetadataBuilder.build());
    }
    return builder.build();
  }
}
