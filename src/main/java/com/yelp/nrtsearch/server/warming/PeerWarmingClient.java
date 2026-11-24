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
package com.yelp.nrtsearch.server.warming;

import com.yelp.nrtsearch.server.grpc.NodeInfo;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.utils.HostPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the warming corpus of a running sibling replica. The primary is asked for its connected
 * replicas, which it lists in the order they connected, so the longest connected replicas, with the
 * most traffic sampled, are tried first. This replica is skipped. Each candidate checks its own
 * uptime and query count and refuses if below the thresholds, then the next one is tried, up to
 * {@link WarmerConfig#getPeerMaxCandidates()} candidates.
 */
public class PeerWarmingClient implements PeerWarmingQuerySource {
  private static final Logger logger = LoggerFactory.getLogger(PeerWarmingClient.class);

  private final String indexName;
  private final ReplicationServerClient primaryClient;
  private final HostPort selfHostPort;
  private final int maxQueries;
  private final WarmerConfig config;
  private final BiFunction<String, Integer, ReplicationServerClient> peerClientFactory;

  /**
   * @param indexName index to fetch queries for
   * @param primaryClient client for the primary, null if there is no primary connection
   * @param selfHostPort replication address of this replica, skipped as a candidate
   * @param config warmer config with the peer settings
   */
  public PeerWarmingClient(
      String indexName,
      ReplicationServerClient primaryClient,
      HostPort selfHostPort,
      WarmerConfig config) {
    this(indexName, primaryClient, selfHostPort, config, ReplicationServerClient::new);
  }

  PeerWarmingClient(
      String indexName,
      ReplicationServerClient primaryClient,
      HostPort selfHostPort,
      WarmerConfig config,
      BiFunction<String, Integer, ReplicationServerClient> peerClientFactory) {
    this.indexName = indexName;
    this.primaryClient = primaryClient;
    this.selfHostPort = selfHostPort;
    this.config = config;
    this.maxQueries = config.getMaxWarmingQueries();
    this.peerClientFactory = peerClientFactory;
  }

  @Override
  public Optional<PeerWarmingQueries> fetch() {
    if (primaryClient == null) {
      logger.info("No primary connection, cannot find peers to warm index: {}", indexName);
      return Optional.empty();
    }
    List<NodeInfo> nodes;
    try {
      nodes = primaryClient.getConnectedNodes(indexName).getNodesList();
    } catch (StatusRuntimeException e) {
      logger.warn(
          "Unable to get replicas of index: {} from primary for peer warming", indexName, e);
      return Optional.empty();
    }
    int tried = 0;
    for (NodeInfo node : nodes) {
      if (isSelf(node)) {
        continue;
      }
      if (tried >= config.getPeerMaxCandidates()) {
        logger.info(
            "Tried {} peers for warming index: {} without success, giving up", tried, indexName);
        break;
      }
      tried++;
      String peer =
          String.format("%s (%s:%d)", node.getNodeName(), node.getHostname(), node.getPort());
      Optional<PeerWarmingQueries> queries = fetchFrom(node, peer);
      if (queries.isPresent()) {
        return queries;
      }
    }
    if (tried == 0) {
      logger.info("No other replicas of index: {} to warm from", indexName);
    }
    return Optional.empty();
  }

  private Optional<PeerWarmingQueries> fetchFrom(NodeInfo node, String peer) {
    try (ReplicationServerClient client =
        peerClientFactory.apply(node.getHostname(), node.getPort())) {
      byte[] content =
          client.getWarmingQueries(
              indexName,
              maxQueries,
              config.getPeerMaxBytes(),
              config.getPeerMinQueries(),
              config.getPeerMinUptimeMinutes(),
              config.getPeerTimeoutMs());
      if (content.length == 0) {
        logger.info("Peer: {} has no warming queries for index: {}", peer, indexName);
        return Optional.empty();
      }
      return Optional.of(new PeerWarmingQueries(peer, content));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
        logger.info(
            "Peer: {} does not qualify for warming index: {}: {}",
            peer,
            indexName,
            e.getStatus().getDescription());
      } else {
        logger.warn(
            "Unable to fetch warming queries for index: {} from peer: {}", indexName, peer, e);
      }
    } catch (RuntimeException e) {
      logger.warn(
          "Unable to fetch warming queries for index: {} from peer: {}", indexName, peer, e);
    }
    return Optional.empty();
  }

  /** Node names may be shared between replicas, so only the replication address identifies one. */
  private boolean isSelf(NodeInfo node) {
    return new HostPort(node.getHostname(), node.getPort()).equals(selfHostPort);
  }
}
