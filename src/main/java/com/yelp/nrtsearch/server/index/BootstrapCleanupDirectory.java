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
package com.yelp.nrtsearch.server.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

/**
 * A {@link FilterDirectory} decorator that cleans up tmpfs-backed bootstrap files when they are
 * deleted by Lucene's file management. During replica bootstrap, index files are downloaded to a
 * tmpfs directory and symlinked into the real index directory. When Lucene's {@link
 * org.apache.lucene.replicator.nrt.ReplicaNode} deletes a file (e.g. after a merge replaces it),
 * {@link org.apache.lucene.store.FSDirectory#deleteFile} removes the symlink but leaves the tmpfs
 * target orphaned. This decorator intercepts {@link #deleteFile} to also delete the tmpfs target,
 * immediately reclaiming memory.
 *
 * <p>Once all bootstrap files have been merged away, the {@code bootstrapFiles} set becomes empty
 * and this decorator becomes a pure passthrough with no overhead.
 */
public class BootstrapCleanupDirectory extends FilterDirectory {

  private final Path bootstrapDir;
  private final Set<String> bootstrapFiles;

  /**
   * @param in the underlying directory (typically {@link org.apache.lucene.store.MMapDirectory})
   * @param bootstrapDir path to the tmpfs directory where bootstrap files reside
   * @param bootstrapFiles thread-safe set of file names that were downloaded to {@code
   *     bootstrapDir} during bootstrap; entries are removed as files are cleaned up
   */
  public BootstrapCleanupDirectory(Directory in, Path bootstrapDir, Set<String> bootstrapFiles) {
    super(in);
    this.bootstrapDir = bootstrapDir;
    this.bootstrapFiles = bootstrapFiles;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    super.deleteFile(name);
    if (bootstrapFiles.remove(name)) {
      Files.deleteIfExists(bootstrapDir.resolve(name));
    }
  }
}
