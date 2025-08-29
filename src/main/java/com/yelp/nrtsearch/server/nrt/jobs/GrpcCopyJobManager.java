/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.nrt.jobs;

import static com.yelp.nrtsearch.server.nrt.NrtUtils.readFilesMetaData;

import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.nrt.NRTReplicaNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.NodeCommunicationException;

/**
 * CopyJobManager implementation that uses gRPC to communicate with the primary to get the CopyState
 * and files to copy.
 */
public class GrpcCopyJobManager implements CopyJobManager {
  private final String indexName;
  private final String indexId;
  private final ReplicationServerClient primaryAddress;
  private final boolean ackedCopy;
  private final NRTReplicaNode replicaNode;
  private final int replicaId;

  public GrpcCopyJobManager(
      String indexName,
      String indexId,
      ReplicationServerClient primaryAddress,
      boolean ackedCopy,
      NRTReplicaNode replicaNode,
      int replicaId) {
    this.indexName = indexName;
    this.indexId = indexId;
    this.primaryAddress = primaryAddress;
    this.ackedCopy = ackedCopy;
    this.replicaNode = replicaNode;
    this.replicaId = replicaId;
  }

  @Override
  public void start() throws IOException {}

  @Override
  public CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException {
    if (!replicaNode.hasPrimaryConnection()) {
      throw new IllegalStateException(
          "Cannot create new copy job, primary connection not available");
    }

    CopyState copyState;

    // sendMeFiles(?) (we dont need this, just send Index,replica, and request for copy State)
    if (files == null) {
      // No incoming CopyState: ask primary for latest one now
      try {
        // Exceptions in here mean something went wrong talking over the socket, which are fine
        // (e.g. primary node crashed):
        copyState = getCopyStateFromPrimary();
      } catch (Throwable t) {
        throw new NodeCommunicationException("exc while reading files to copy", t);
      }
      files = copyState.files();
    } else {
      copyState = null;
    }
    return new SimpleCopyJob(
        reason,
        primaryAddress,
        copyState,
        replicaNode,
        files,
        highPriority,
        onceDone,
        indexName,
        indexId,
        ackedCopy);
  }

  @Override
  public void finishNRTCopy(CopyJob copyJob) throws IOException {}

  private CopyState getCopyStateFromPrimary() throws IOException {
    com.yelp.nrtsearch.server.grpc.CopyState copyState =
        primaryAddress.recvCopyState(indexName, indexId, replicaId);
    return readCopyState(copyState);
  }

  /** Pulls CopyState off the wire */
  private static CopyState readCopyState(com.yelp.nrtsearch.server.grpc.CopyState copyState)
      throws IOException {

    // Decode a new CopyState
    byte[] infosBytes = new byte[copyState.getInfoBytesLength()];
    copyState.getInfoBytes().copyTo(ByteBuffer.wrap(infosBytes));

    long gen = copyState.getGen();
    long version = copyState.getVersion();
    Map<String, FileMetaData> files = readFilesMetaData(copyState.getFilesMetadata());

    Set<String> completedMergeFiles = new HashSet<>(copyState.getCompletedMergeFilesList());
    long primaryGen = copyState.getPrimaryGen();

    return new CopyState(files, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
  }

  @Override
  public void close() throws IOException {}
}
