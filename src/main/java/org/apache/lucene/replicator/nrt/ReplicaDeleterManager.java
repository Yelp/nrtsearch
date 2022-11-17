/*
 * Copyright 2022 Yelp Inc.
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
package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import org.apache.lucene.index.SegmentInfos;

public class ReplicaDeleterManager {

  private final ReplicaNode replicaNode;

  public ReplicaDeleterManager(ReplicaNode replicaNode) {
    this.replicaNode = replicaNode;
  }

  /*
   * This method deletes the files in last committed files which are no longer used.
   * Unfortunately we cannot modify the last committed files list in ReplicaNode
   */
  public void decReplicaInitialCommitFiles() throws IOException {
    synchronized (replicaNode.commitLock) {
      synchronized (replicaNode) {
        Collection<String> removableFiles = new HashSet<>();
        String segmentsFileName =
            SegmentInfos.getLastCommitSegmentsFileName(replicaNode.getDirectory());
        if (segmentsFileName != null) {
          Collection<String> indexFiles =
              SegmentInfos.readCommit(replicaNode.getDirectory(), segmentsFileName).files(false);
          removableFiles.addAll(indexFiles);
          removableFiles.add(segmentsFileName);
          replicaNode.deleter.decRef(removableFiles);
        }
      }
    }
  }
}
