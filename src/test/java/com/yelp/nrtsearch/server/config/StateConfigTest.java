/*
 * Copyright 2022 Yelp Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.StateConfig.StateBackendType;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class StateConfigTest {
  private static StateConfig getConfig(String configFile) {
    return StateConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefaultConfig() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    StateConfig stateConfig = getConfig(configFile);
    assertEquals(StateBackendType.LOCAL, stateConfig.getBackendType());
  }

  @Test
  public void testValidConfig() {
    String configFile = String.join("\n", "stateConfig:", "  backendType: LOCAL");
    StateConfig stateConfig = getConfig(configFile);
    assertEquals(StateBackendType.LOCAL, stateConfig.getBackendType());
  }

  @Test
  public void testInvalidBackendType() {
    String configFile = String.join("\n", "stateConfig:", "  backendType: INVALID");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("No enum constant"));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfigReader() {
    StateConfig.fromConfig(null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullBackendType() {
    new StateConfig(null);
  }
}
