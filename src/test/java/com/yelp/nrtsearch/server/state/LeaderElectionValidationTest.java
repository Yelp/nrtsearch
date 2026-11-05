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
package com.yelp.nrtsearch.server.state;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class LeaderElectionValidationTest {
  private static final String VALID =
      String.join(
          "\n",
          "nodeName: n",
          "hostName: primary-1.example.com",
          "s3RefreshUpload: true",
          "s3RefreshNotifyAfterUpload: true",
          "stateConfig:",
          "  backendType: REMOTE",
          "indexStartConfig:",
          "  mode: PRIMARY",
          "  dataLocationType: REMOTE",
          "leaderElection:",
          "  type: s3_lease");

  private static NrtsearchConfig config(String yaml) {
    return new NrtsearchConfig(new ByteArrayInputStream(yaml.getBytes()));
  }

  @Test
  public void testValidConfig() {
    BackendGlobalState.validateLeaderElectionConfig(config(VALID));
  }

  @Test
  public void testLoopbackHostRejected() {
    for (String host : new String[] {"localhost", "127.0.0.1", "0.0.0.0", "::1"}) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  BackendGlobalState.validateLeaderElectionConfig(
                      config(VALID.replace("primary-1.example.com", host))));
      assertTrue(e.getMessage(), e.getMessage().contains("hostName must be reachable"));
    }
  }

  @Test
  public void testLoopbackHostAllowedWhenConfigured() {
    BackendGlobalState.validateLeaderElectionConfig(
        config(
            VALID.replace("primary-1.example.com", "localhost") + "\n  allowLoopbackHost: true"));
  }

  @Test
  public void testIsLoopbackHost() {
    assertTrue(BackendGlobalState.isLoopbackHost(""));
    assertTrue(BackendGlobalState.isLoopbackHost("LocalHost"));
    assertTrue(BackendGlobalState.isLoopbackHost("127.0.1.1"));
    assertFalse(BackendGlobalState.isLoopbackHost("10.0.0.1"));
    assertFalse(BackendGlobalState.isLoopbackHost("primary-1"));
  }

  private static IndexStartConfig startConfig(String discovery) {
    return IndexStartConfig.fromConfig(
        new YamlConfigReader(
            new ByteArrayInputStream(
                ("indexStartConfig:\n  mode: REPLICA\n  primaryDiscovery:\n" + discovery)
                    .getBytes())));
  }

  @Test
  public void testLeaderRpcSingleHostRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> startConfig("    resolver: LEADER_RPC\n    host: primary-1\n    port: 9000\n"));
    assertTrue(e.getMessage(), e.getMessage().contains("single host"));
  }

  @Test
  public void testLeaderRpcHostListAccepted() {
    IndexStartConfig c =
        startConfig("    resolver: LEADER_RPC\n    host: primary-1, primary-2\n    port: 9000\n");
    assertTrue(c.getPrimaryResolver() == IndexStartConfig.PrimaryResolverType.LEADER_RPC);
    assertTrue(
        IndexStartConfig.splitHosts(c.getDiscoveryHost())
            .equals(java.util.List.of("primary-1", "primary-2")));
  }

  @Test
  public void testLeaderRpcSingleHostAllowedForLoadBalancer() {
    startConfig(
        "    resolver: LEADER_RPC\n    host: primaries.lb\n    port: 9000\n"
            + "    allowSingleLeaderSeed: true\n");
  }

  @Test
  public void testLeaderRpcFileAccepted() {
    startConfig("    resolver: LEADER_RPC\n    file: /tmp/primaries.json\n");
  }

  @Test
  public void testFileResolverSingleHostUnchanged() {
    startConfig("    host: primary-1\n    port: 9000\n");
  }
}
