/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import org.junit.Test;

public class IsolatedReplicaConfigTest {

  private static IsolatedReplicaConfig getConfig(String configFile) {
    return IsolatedReplicaConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefaultValues() {
    String configFile = "nodeName: \"server_foo\"";
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0); // default value
  }

  @Test
  public void testEnabledTrue() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  enabled: true");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120); // default value
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0); // default value
  }

  @Test
  public void testEnabledFalse() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  enabled: false");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120); // default value
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0); // default value
  }

  @Test
  public void testCustomPollingInterval() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  pollingIntervalSeconds: 60");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse(); // default value
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(60);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0); // default value
  }

  @Test
  public void testBothValuesSet() {
    String configFile =
        String.join(
            "\n", "isolatedReplicaConfig:", "  enabled: true", "  pollingIntervalSeconds: 30");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(30);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0); // default value
  }

  @Test
  public void testZeroPollingInterval() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  pollingIntervalSeconds: 0");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("Polling interval seconds must be positive, got: 0");
    }
  }

  @Test
  public void testNegativePollingInterval() {
    String configFile =
        String.join("\n", "isolatedReplicaConfig:", "  pollingIntervalSeconds: -10");
    try {
      getConfig(configFile);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("Polling interval seconds must be positive, got: -10");
    }
  }

  @Test
  public void testLargePollingInterval() {
    String configFile =
        String.join("\n", "isolatedReplicaConfig:", "  pollingIntervalSeconds: 3600");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(3600);
  }

  @Test
  public void testStringBooleanEnabled() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  enabled: \"true\"");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120);
  }

  @Test
  public void testStringBooleanDisabled() {
    String configFile = String.join("\n", "isolatedReplicaConfig:", "  enabled: \"false\"");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120);
  }

  @Test
  public void testStringPollingInterval() {
    String configFile =
        String.join("\n", "isolatedReplicaConfig:", "  pollingIntervalSeconds: \"90\"");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(90);
  }

  @Test
  public void testConstructorWithEnabledTrue() {
    IsolatedReplicaConfig config = new IsolatedReplicaConfig(true, 240, 0);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(240);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0);
  }

  @Test
  public void testConstructorWithEnabledFalse() {
    IsolatedReplicaConfig config = new IsolatedReplicaConfig(false, 15, 0);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(15);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0);
  }

  @Test
  public void testConstructorWithZeroInterval() {
    try {
      new IsolatedReplicaConfig(true, 0, 0);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("Polling interval seconds must be positive, got: 0");
    }
  }

  @Test
  public void testConstructorWithNegativeInterval() {
    try {
      new IsolatedReplicaConfig(false, -5, 0);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("Polling interval seconds must be positive, got: -5");
    }
  }

  @Test
  public void testConstructorWithNegativeFreshnessTarget() {
    try {
      new IsolatedReplicaConfig(true, 120, -10);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage())
          .isEqualTo("Freshness target seconds must be non-negative, got: -10");
    }
  }

  @Test
  public void testFromConfigWithNullConfigReader() {
    assertThatThrownBy(() -> IsolatedReplicaConfig.fromConfig(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testFromConfigWithMockConfigReader() {
    YamlConfigReader mockReader = mock(YamlConfigReader.class);
    when(mockReader.getBoolean("isolatedReplicaConfig.enabled", false)).thenReturn(true);
    when(mockReader.getInteger("isolatedReplicaConfig.pollingIntervalSeconds", 120)).thenReturn(45);
    when(mockReader.getInteger("isolatedReplicaConfig.freshnessTargetSeconds", 0)).thenReturn(60);

    IsolatedReplicaConfig config = IsolatedReplicaConfig.fromConfig(mockReader);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(45);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(60);
  }

  @Test
  public void testFromConfigWithMockConfigReaderDefaults() {
    YamlConfigReader mockReader = mock(YamlConfigReader.class);
    when(mockReader.getBoolean("isolatedReplicaConfig.enabled", false)).thenReturn(false);
    when(mockReader.getInteger("isolatedReplicaConfig.pollingIntervalSeconds", 120))
        .thenReturn(120);
    when(mockReader.getInteger("isolatedReplicaConfig.freshnessTargetSeconds", 0)).thenReturn(0);

    IsolatedReplicaConfig config = IsolatedReplicaConfig.fromConfig(mockReader);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0);
  }

  @Test
  public void testConfigPrefix() {
    assertThat(IsolatedReplicaConfig.CONFIG_PREFIX).isEqualTo("isolatedReplicaConfig.");
  }

  @Test
  public void testEmptyConfig() {
    String configFile = "{}";
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(0);
  }

  @Test
  public void testComplexYamlWithOtherSettings() {
    String configFile =
        String.join(
            "\n",
            "nodeName: \"test_node\"",
            "port: 9090",
            "isolatedReplicaConfig:",
            "  enabled: true",
            "  pollingIntervalSeconds: 180",
            "  freshnessTargetSeconds: 300",
            "otherConfig:",
            "  someValue: 42");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(180);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(300);
  }

  @Test
  public void testConfigWithComments() {
    String configFile =
        String.join(
            "\n",
            "# Node configuration",
            "nodeName: \"test_node\"",
            "# Isolated replica settings",
            "isolatedReplicaConfig:",
            "  enabled: true  # Enable isolated replica",
            "  pollingIntervalSeconds: 300  # Poll every 5 minutes",
            "  freshnessTargetSeconds: 120  # Allow up to 2 minutes staleness");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(300);
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(120);
  }

  @Test
  public void testCustomFreshnessTarget() {
    String configFile =
        String.join("\n", "isolatedReplicaConfig:", "  freshnessTargetSeconds: 240");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isFalse(); // default value
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120); // default value
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(240); // configured value stored
  }

  @Test
  public void testCustomFreshnessTargetWithEnabled() {
    String configFile =
        String.join(
            "\n", "isolatedReplicaConfig:", "  enabled: true", "  freshnessTargetSeconds: 240");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120); // default value
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(240);
  }

  @Test
  public void testCustomFreshnessTargetTooLarge() {
    String configFile =
        String.join(
            "\n", "isolatedReplicaConfig:", "  enabled: true", "  freshnessTargetSeconds: 86401");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("Freshness target seconds must be <= 86400, got: 86401");
    }
  }

  @Test
  public void testStringFreshnessTarget() {
    String configFile =
        String.join(
            "\n", "isolatedReplicaConfig:", "  enabled: true", "  freshnessTargetSeconds: \"180\"");
    IsolatedReplicaConfig config = getConfig(configFile);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getPollingIntervalSeconds()).isEqualTo(120); // default value
    assertThat(config.getFreshnessTargetSeconds()).isEqualTo(180);
  }
}
