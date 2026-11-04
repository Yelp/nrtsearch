/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.tools.vector_bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.yelp.nrtsearch.vectors.IndexConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import picocli.CommandLine;

@CommandLine.Command(
    name = "indexdir",
    description =
        "Print a node's data directory for the current version of the index, from the node's local"
            + " copy of the cluster state. With --stale, print directories of earlier versions"
            + " instead, which a node keeps after the index is recreated.")
public class IndexDirCommand implements Callable<Integer> {
  @CommandLine.Option(
      names = {"--stateDir"},
      description = "The node's stateDir",
      required = true)
  Path stateDir;

  @CommandLine.Option(
      names = {"--indexDir"},
      description = "The node's indexDir",
      required = true)
  Path indexDir;

  @CommandLine.Option(
      names = {"--bucket"},
      description = "Vector bucket name (default: ${DEFAULT-VALUE})",
      defaultValue = "bench")
  String bucket;

  @CommandLine.Option(
      names = {"--index"},
      description = "Vector index name (default: ${DEFAULT-VALUE})",
      defaultValue = "vectors")
  String index;

  @CommandLine.Option(
      names = {"--stale"},
      description = "Print directories of other versions of the index")
  boolean stale;

  @Override
  public Integer call() throws Exception {
    String nrtIndex = IndexConfig.nrtsearchIndexName(bucket, index);
    String current = currentDirName(stateDir, nrtIndex);
    if (!stale) {
      if (current == null) {
        System.err.println("error: no index " + nrtIndex + " in " + stateFile(stateDir));
        return 1;
      }
      System.out.println(indexDir.resolve(current));
      return 0;
    }
    for (Path dir : versionDirs(indexDir, nrtIndex)) {
      if (!dir.getFileName().toString().equals(current)) {
        System.out.println(dir);
      }
    }
    return 0;
  }

  static Path stateFile(Path stateDir) {
    return stateDir.resolve("global_state").resolve("state.json");
  }

  /** Directory name of the current version, {@code <index>-<id>}, or null if the index is gone. */
  static String currentDirName(Path stateDir, String nrtIndex) throws Exception {
    Path file = stateFile(stateDir);
    if (!Files.exists(file)) {
      return null;
    }
    JsonNode id =
        BenchClient.MAPPER.readTree(file.toFile()).path("indices").path(nrtIndex).path("id");
    return id.isTextual() ? nrtIndex + "-" + id.asText() : null;
  }

  /** Every version directory of the index under indexDir. */
  static List<Path> versionDirs(Path indexDir, String nrtIndex) throws Exception {
    List<Path> dirs = new ArrayList<>();
    if (!Files.isDirectory(indexDir)) {
      return dirs;
    }
    try (Stream<Path> children = Files.list(indexDir)) {
      children
          .filter(Files::isDirectory)
          .filter(p -> p.getFileName().toString().startsWith(nrtIndex + "-"))
          .sorted()
          .forEach(dirs::add);
    }
    return dirs;
  }
}
