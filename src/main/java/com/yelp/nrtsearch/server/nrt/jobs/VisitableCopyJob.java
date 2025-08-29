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

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.ReplicaNode;

/**
 * Extension of {@link CopyJob} that supports a visit operation to run a unit of work to copy the
 * file. This is used by {@link com.yelp.nrtsearch.server.nrt.NrtCopyThread}.
 */
public abstract class VisitableCopyJob extends CopyJob {
  protected VisitableCopyJob(
      String reason,
      Map<String, FileMetaData> files,
      ReplicaNode dest,
      boolean highPriority,
      OnceDone onceDone)
      throws IOException {
    super(reason, files, dest, highPriority, onceDone);
  }

  /**
   * Perform a unit of work to copy the file.
   *
   * @return true if the file is fully copied, false if more work remains
   * @throws IOException on error
   */
  public abstract boolean visit() throws IOException;
}
