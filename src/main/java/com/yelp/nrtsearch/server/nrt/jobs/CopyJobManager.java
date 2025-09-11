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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;

/** Manages the creation and completion of {@link CopyJob}s. */
public interface CopyJobManager extends Closeable {
  /**
   * Start the copy job manager, initializing any resources needed. This is called at the end of the
   * {@link com.yelp.nrtsearch.server.nrt.NRTReplicaNode} startup process.
   *
   * @throws IOException on error
   */
  void start() throws IOException;

  /**
   * Create a new {@link CopyJob} to copy the needed files to the replica.
   *
   * @param reason the reason for the copy job
   * @param files the files to be copied
   * @param prevFiles the previous files on the replica
   * @param highPriority if true, the copy job should be prioritized (nrt copy)
   * @param onceDone callback to be called once the copy job is done
   * @return the new copy job
   * @throws IOException on error
   */
  CopyJob newCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      Map<String, FileMetaData> prevFiles,
      boolean highPriority,
      CopyJob.OnceDone onceDone)
      throws IOException;

  /**
   * Called in the {@link com.yelp.nrtsearch.server.nrt.NRTReplicaNode} finishNRTCopy method to
   * allow the copy job manager to perform any needed finalization after an nrt copy job is
   * completed.
   *
   * @param copyJob the completed copy job
   * @throws IOException on error
   */
  void finishNRTCopy(CopyJob copyJob) throws IOException;
}
