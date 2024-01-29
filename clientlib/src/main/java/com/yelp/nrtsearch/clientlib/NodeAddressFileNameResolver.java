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
package com.yelp.nrtsearch.clientlib;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link NameResolver} that works with a json file containing lists of {@link Node}s. */
public class NodeAddressFileNameResolver extends NameResolver {
  private static final Logger logger = LoggerFactory.getLogger(NodeAddressFileNameResolver.class);

  private final File nodeAddressesFile;
  private final ObjectMapper objectMapper;
  private final int updateInterval;
  private Listener listener;
  private Timer fileChangeCheckTimer;
  private List<Node> currentNodes;

  public NodeAddressFileNameResolver(URI fileUri, ObjectMapper objectMapper, int updateIntervalMs) {
    this.nodeAddressesFile = new File(fileUri.getPath());
    this.objectMapper = objectMapper;
    this.updateInterval = updateIntervalMs;
  }

  @Override
  public String getServiceAuthority() {
    // fileUri will always have null authority
    return "";
  }

  @Override
  public void start(Listener listener) {
    this.listener = listener;
    loadNodes();

    // Run timer thread in background
    fileChangeCheckTimer = new Timer("NodeAddressFileNameResolverThread", true);

    fileChangeCheckTimer.schedule(new FileChangedTask(), updateInterval, updateInterval);
  }

  @Override
  public void shutdown() {
    if (fileChangeCheckTimer != null) {
      fileChangeCheckTimer.cancel();
    }
  }

  private void loadNodes() {
    List<Node> nodes = readNodesFromFile();
    updateNodes(nodes);
    currentNodes = nodes;
  }

  private void updateNodes(List<Node> nodes) {
    List<EquivalentAddressGroup> addrs =
        nodes.stream()
            .map(node -> (SocketAddress) new InetSocketAddress(node.getHost(), node.getPort()))
            .map(Collections::singletonList)
            .map(EquivalentAddressGroup::new)
            .collect(Collectors.toList());
    this.listener.onAddresses(addrs, Attributes.EMPTY);
  }

  private List<Node> readNodesFromFile() {
    try {
      List<Node> nodes =
          objectMapper.readValue(nodeAddressesFile, new TypeReference<List<Node>>() {});
      return nodes == null ? Collections.emptyList() : nodes;
    } catch (IOException e) {
      logger.warn("Unable to read file: {}", nodeAddressesFile, e);
      return Collections.emptyList();
    }
  }

  private class FileChangedTask extends TimerTask {

    public FileChangedTask() {
      currentNodes = readNodesFromFile();
    }

    @Override
    public void run() {
      List<Node> nodes = readNodesFromFile();
      if (!nodes.isEmpty() && !Objects.equals(nodes, currentNodes)) {
        updateNodes(nodes);
        currentNodes = nodes;
      }
    }
  }
}
