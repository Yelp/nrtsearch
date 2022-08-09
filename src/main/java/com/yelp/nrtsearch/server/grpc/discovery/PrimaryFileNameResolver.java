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
package com.yelp.nrtsearch.server.grpc.discovery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.Node;
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

/**
 * {@link NameResolver} that works with a json file containing primary {@link Node}. Ensures that
 * only hosts are only updated when file contains exactly one Node. Allows for port override to be
 * supplied.
 */
public class PrimaryFileNameResolver extends NameResolver {
  private static final Logger logger = LoggerFactory.getLogger(PrimaryFileNameResolver.class);

  private final File nodeAddressesFile;
  private final ObjectMapper objectMapper;
  private final int updateInterval;
  private final int portOverride;
  private Listener listener;
  private Timer fileChangeCheckTimer;
  private List<Node> currentNodes;

  /**
   * Constructor.
   *
   * @param fileUri location of discovery file
   * @param objectMapper jackson object mapper to read discovery file
   * @param updateIntervalMs how often to check for updated hosts
   * @param portOverride if > 0, replaces port value from discovery file
   */
  public PrimaryFileNameResolver(
      URI fileUri, ObjectMapper objectMapper, int updateIntervalMs, int portOverride) {
    this.nodeAddressesFile = new File(fileUri.getPath());
    this.objectMapper = objectMapper;
    this.updateInterval = updateIntervalMs;
    this.portOverride = portOverride;
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
    fileChangeCheckTimer = new Timer("PrimaryFileNameResolverThread", true);

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
    if (portOverride > 0) {
      nodes =
          nodes.stream()
              .map(node -> new Node(node.getHost(), portOverride))
              .collect(Collectors.toList());
    }
    logger.info("Discovered primaries: " + nodes);
    if (nodes.size() <= 1) {
      updateNodes(nodes);
    } else {
      // there can only be one active primary, better to wait until this resolves
      updateNodes(Collections.emptyList());
    }
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
      List<Node> nodes = objectMapper.readValue(nodeAddressesFile, new TypeReference<>() {});
      return nodes == null ? Collections.emptyList() : nodes;
    } catch (IOException e) {
      logger.warn("Unable to read file: {}", nodeAddressesFile, e);
      return Collections.emptyList();
    }
  }

  class FileChangedTask extends TimerTask {

    public FileChangedTask() {}

    @Override
    public void run() {
      List<Node> nodes = readNodesFromFile();
      if (portOverride > 0) {
        nodes =
            nodes.stream()
                .map(node -> new Node(node.getHost(), portOverride))
                .collect(Collectors.toList());
      }
      if (!Objects.equals(nodes, currentNodes)) {
        logger.info("Discovered new primaries: " + nodes);
        if (nodes.size() <= 1) {
          updateNodes(nodes);
        } else {
          // there can only be one active primary, better to wait until this resolves
          updateNodes(Collections.emptyList());
        }
        currentNodes = nodes;
      }
    }
  }
}
