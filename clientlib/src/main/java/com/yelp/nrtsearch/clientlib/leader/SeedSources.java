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
package com.yelp.nrtsearch.clientlib.leader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.Node;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sources of seed nodes for {@link LeaderResolver}. */
public final class SeedSources {
  private static final Logger logger = LoggerFactory.getLogger(SeedSources.class);

  private SeedSources() {}

  /** A fixed list of seed nodes. */
  public static Supplier<Collection<Node>> fixed(List<Node> nodes) {
    List<Node> copy = List.copyOf(nodes);
    return () -> copy;
  }

  /**
   * Seed nodes from a json file containing a list of {@link Node}s, read on every call so file
   * updates are picked up. An unreadable file gives no seeds.
   *
   * @param mapper object mapper
   * @param file path to the file
   * @param portOverride if > 0, replaces the port of every node
   */
  public static Supplier<Collection<Node>> file(
      ObjectMapper mapper, String file, int portOverride) {
    File seedFile = new File(file);
    return () -> {
      try {
        List<Node> nodes = mapper.readValue(seedFile, new TypeReference<List<Node>>() {});
        if (nodes == null) {
          return Collections.emptyList();
        }
        if (portOverride > 0) {
          return nodes.stream()
              .map(n -> new Node(n.getHost(), portOverride))
              .collect(Collectors.toList());
        }
        return nodes;
      } catch (Exception e) {
        logger.warn("Unable to read leader seed file: {}", file, e);
        return Collections.emptyList();
      }
    };
  }
}
