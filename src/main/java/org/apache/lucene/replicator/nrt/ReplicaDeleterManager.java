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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicaDeleterManager {

  private final ReplicaNode replicaNode;

  public ReplicaDeleterManager(ReplicaNode replicaNode) {
    this.replicaNode = replicaNode;
  }

  /*
   * This method deletes the files in last committed files which are no longer used.
   * Unfortunately we cannot modify the last committed files list in ReplicaNode
   */
  public void cleanUpReplicaFiles() throws IOException {
    synchronized (replicaNode.commitLock) {
      String[] allFilesInDir;
      Collection<String> indexFiles;
      synchronized (replicaNode) {
        allFilesInDir = replicaNode.dir.listAll();
        indexFiles = ((SegmentInfosSearcherManager) replicaNode.mgr).getCurrentInfos().files(true);
      }
      List<String> removableFiles =
          Arrays.stream(allFilesInDir)
              .filter(f -> isRemovable(f, indexFiles))
              .collect(Collectors.toList());
      replicaNode.deleter.decRef(removableFiles);
    }
  }

  private boolean isRemovable(String fileName, Collection<String> activeFiles) {
    return (!activeFiles.contains(fileName))
        && (!replicaNode.lastNRTFiles.contains(fileName))
        && (!replicaNode.pendingMergeFiles.contains(fileName));
  }
}
