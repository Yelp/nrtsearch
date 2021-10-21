/*
 * Copyright 2021 Yelp Inc.
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
package com.yelp.nrtsearch.server.backup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class DirectorySegmenter {
  public static class FileBySize implements Comparator<FileBySize> {
    private Path fileName;
    private long size;

    FileBySize(Path fileName, long size) {
      this.fileName = fileName;
      this.size = size;
    }

    @Override
    public int compare(FileBySize o1, FileBySize o2) {
      return Long.compare(o2.size, o1.size);
    }
  }

  PriorityQueue<FileBySize> pq = new PriorityQueue<>();

  public List<List<Path>> chunkDirectory(Path dirToChunk, long chunkSizeLimit) throws IOException {
    for (File file : dirToChunk.toFile().listFiles()) {
      checkIfDir(file);
      pq.add(new FileBySize(file.toPath(), file.length()));
    }
    long currentChunkSize = 0l;
    List<List<Path>> chunkedFiles = new ArrayList<>();
    List<Path> singleChunk = new ArrayList<>();
    while (!pq.isEmpty()) {
      if (currentChunkSize < chunkSizeLimit) {
        FileBySize fileBySize = pq.poll();
        singleChunk.add(fileBySize.fileName);
        currentChunkSize += fileBySize.size;
      } else {
        currentChunkSize = 0l;
        chunkedFiles.add(singleChunk);
        singleChunk = new ArrayList<>();
      }
    }
    return chunkedFiles;
  }

  private void checkIfDir(File toFile) {
    if (toFile.isDirectory()) {
      throw new UnsupportedOperationException("Nested directory size chunking is not supported");
    }
  }
}
