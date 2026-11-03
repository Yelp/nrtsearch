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
package com.yelp.nrtsearch.server.leader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.plugins.PrimaryElectorPlugin;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PrimaryElectorCreatorTest {
  private static final PrimaryElector TEST_ELECTOR = mock(PrimaryElector.class);

  static class TestElectorPlugin extends Plugin implements PrimaryElectorPlugin {
    @Override
    public Map<String, PrimaryElectorProvider> getPrimaryElectors() {
      return Map.of(
          "test_elector",
          (context, params) -> {
            assertEquals("v", params.get("k"));
            return TEST_ELECTOR;
          });
    }
  }

  static class ReservedNamePlugin extends Plugin implements PrimaryElectorPlugin {
    @Override
    public Map<String, PrimaryElectorProvider> getPrimaryElectors() {
      return Map.of("none", (c, p) -> TEST_ELECTOR);
    }
  }

  private static NrtsearchConfig config(String yaml) {
    return new NrtsearchConfig(new ByteArrayInputStream(("nodeName: n\n" + yaml).getBytes()));
  }

  private static PrimaryElectorContext context(NrtsearchConfig config, RemoteBackend backend) {
    return new PrimaryElectorContext(
        config, new LeaderInfo("id", "n", "h", 1, 2, -1), backend, null);
  }

  @Test
  public void testDisabledByDefault() {
    NrtsearchConfig config = config("");
    assertFalse(config.getLeaderElectionConfig().isEnabled());
    assertFalse(config.getEnforcePrimaryGenOrdering());
    PrimaryElectorCreator.initialize(config, List.of());
    assertTrue(PrimaryElectorCreator.getInstance().createElector(context(config, null)).isEmpty());
  }

  @Test
  public void testPluginElector() {
    NrtsearchConfig config = config("leaderElection:\n  type: test_elector\n  params:\n    k: v\n");
    assertTrue(config.getEnforcePrimaryGenOrdering());
    PrimaryElectorCreator.initialize(config, List.of(new TestElectorPlugin()));
    assertSame(
        TEST_ELECTOR,
        PrimaryElectorCreator.getInstance().createElector(context(config, null)).get());
  }

  @Test
  public void testUnknownType() {
    NrtsearchConfig config = config("leaderElection:\n  type: missing\n");
    PrimaryElectorCreator.initialize(config, List.of());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> PrimaryElectorCreator.getInstance().createElector(context(config, null)));
    assertTrue(e.getMessage().contains("Unknown leader election type: missing"));
  }

  @Test
  public void testS3LeaseRequiresS3Backend() {
    NrtsearchConfig config = config("leaderElection:\n  type: s3_lease\n");
    PrimaryElectorCreator.initialize(config, List.of());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PrimaryElectorCreator.getInstance()
                    .createElector(context(config, mock(RemoteBackend.class))));
    assertTrue(e.getMessage().contains("requires an S3 remote backend"));
  }

  @Test
  public void testDuplicateAndReservedNames() {
    NrtsearchConfig config = config("");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PrimaryElectorCreator.initialize(
                config, List.of(new TestElectorPlugin(), new TestElectorPlugin())));
    assertThrows(
        IllegalArgumentException.class,
        () -> PrimaryElectorCreator.initialize(config, List.of(new ReservedNamePlugin())));
  }

  @Test
  public void testExplicitEnforceOverride() {
    NrtsearchConfig config =
        config("enforcePrimaryGenOrdering: false\nleaderElection:\n  type: s3_lease\n");
    assertFalse(config.getEnforcePrimaryGenOrdering());
  }
}
