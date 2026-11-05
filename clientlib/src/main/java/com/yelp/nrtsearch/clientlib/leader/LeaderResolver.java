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

import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the active primary of a multi-primary cluster by asking seed nodes with resolveLeader.
 *
 * <p>Every seed is asked for the leader it knows. The answer with the highest epoch is a candidate;
 * it is only accepted if the candidate itself confirms, at that same epoch, that it is the leader.
 * A node that missed a failover reports an older epoch and loses to newer answers, and a deposed
 * leader no longer confirms. If no candidate is confirmed, no leader is returned and callers keep
 * their current target rather than switching to an unconfirmed one.
 */
public class LeaderResolver {
  private static final Logger logger = LoggerFactory.getLogger(LeaderResolver.class);

  /** Calls resolveLeader on a node. */
  @FunctionalInterface
  public interface Probe {
    /**
     * @param node node to ask
     * @return the node's answer
     * @throws Exception if the node can not be reached or knows no leader
     */
    ResolveLeaderResponse resolveLeader(Node node) throws Exception;
  }

  private final Supplier<Collection<Node>> seeds;
  private final Probe probe;
  private final Function<ResolveLeaderResponse, Node> leaderAddress;

  /**
   * Constructor.
   *
   * @param seeds nodes to ask; read on every resolve, so it may change (e.g. a discovery file)
   * @param probe calls resolveLeader on a node
   * @param leaderAddress address to reach the leader at, from its answer (client or replication
   *     port)
   */
  public LeaderResolver(
      Supplier<Collection<Node>> seeds,
      Probe probe,
      Function<ResolveLeaderResponse, Node> leaderAddress) {
    this.seeds = Objects.requireNonNull(seeds);
    this.probe = Objects.requireNonNull(probe);
    this.leaderAddress = Objects.requireNonNull(leaderAddress);
  }

  /** Leader address from a client service answer. */
  public static Node clientAddress(ResolveLeaderResponse response) {
    return new Node(response.getHost(), response.getPort());
  }

  /** Leader address from a replication service answer. */
  public static Node replicationAddress(ResolveLeaderResponse response) {
    return new Node(response.getHost(), response.getReplicationPort());
  }

  /**
   * Resolve the confirmed leader.
   *
   * @return the leader's address and epoch, or empty if no leader could be confirmed
   */
  public Optional<Resolved> resolve() {
    ResolveLeaderResponse best = null;
    for (Node seed : seeds.get()) {
      try {
        ResolveLeaderResponse response = probe.resolveLeader(seed);
        if (best == null || response.getEpoch() > best.getEpoch()) {
          best = response;
        }
      } catch (Exception e) {
        logger.debug("resolveLeader failed for seed {}:{}", seed.getHost(), seed.getPort(), e);
      }
    }
    if (best == null) {
      return Optional.empty();
    }
    Node candidate = leaderAddress.apply(best);
    try {
      ResolveLeaderResponse confirm = probe.resolveLeader(candidate);
      if (confirm.getIsLeader() && confirm.getEpoch() == best.getEpoch()) {
        return Optional.of(new Resolved(candidate, best.getEpoch()));
      }
      logger.info(
          "Leader {} at epoch {} not confirmed (isLeader={}, epoch={})",
          best.getNodeName(),
          best.getEpoch(),
          confirm.getIsLeader(),
          confirm.getEpoch());
    } catch (Exception e) {
      logger.info(
          "Could not confirm leader {} at {}:{}",
          best.getNodeName(),
          candidate.getHost(),
          candidate.getPort(),
          e);
    }
    return Optional.empty();
  }

  /** A confirmed leader. */
  public static final class Resolved {
    private final Node address;
    private final long epoch;

    public Resolved(Node address, long epoch) {
      this.address = Objects.requireNonNull(address);
      this.epoch = epoch;
    }

    /** Address to reach the leader. */
    public Node address() {
      return address;
    }

    /** Leadership epoch. */
    public long epoch() {
      return epoch;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Resolved)) {
        return false;
      }
      Resolved other = (Resolved) o;
      return epoch == other.epoch && address.equals(other.address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(address, epoch);
    }

    @Override
    public String toString() {
      return address + "@" + epoch;
    }
  }
}
