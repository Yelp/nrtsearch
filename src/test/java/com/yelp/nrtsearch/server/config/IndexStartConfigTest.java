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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class IndexStartConfigTest {

  private static IndexStartConfig getConfig(String configFile) {
    return IndexStartConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefaultConfig() {
    String configFile = "nodeName: \"lucene_server_foo\"";
    IndexStartConfig startConfig = getConfig(configFile);
    assertFalse(startConfig.getAutoStart());
    assertEquals(Mode.STANDALONE, startConfig.getMode());
    assertEquals("", startConfig.getDiscoveryHost());
    assertEquals(Integer.valueOf(0), startConfig.getDiscoveryPort());
    assertEquals("", startConfig.getDiscoveryFile());
    assertEquals(IndexDataLocationType.LOCAL, startConfig.getDataLocationType());
  }

  @Test
  public void testValidConfig() {
    String configFile =
        String.join(
            "\n",
            "indexStartConfig:",
            "  autoStart: true",
            "  mode: REPLICA",
            "  dataLocationType: REMOTE",
            "  primaryDiscovery:",
            "    host: some_host",
            "    port: 1111",
            "    file: some/file/path");
    IndexStartConfig startConfig = getConfig(configFile);
    assertTrue(startConfig.getAutoStart());
    assertEquals(Mode.REPLICA, startConfig.getMode());
    assertEquals("some_host", startConfig.getDiscoveryHost());
    assertEquals(Integer.valueOf(1111), startConfig.getDiscoveryPort());
    assertEquals("some/file/path", startConfig.getDiscoveryFile());
    assertEquals(IndexDataLocationType.REMOTE, startConfig.getDataLocationType());
  }

  @Test
  public void testInvalidMode() {
    String configFile = String.join("\n", "indexStartConfig:", "  mode: INVALID");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("No enum constant"));
    }
  }

  @Test
  public void testInvalidLocationType() {
    String configFile = String.join("\n", "indexStartConfig:", "  dataLocationType: INVALID");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("No enum constant"));
    }
  }

  @Test
  public void testStandaloneWithRemote() {
    String configFile =
        String.join("\n", "indexStartConfig:", "  dataLocationType: REMOTE", "  mode: STANDALONE");
    try {
      getConfig(configFile);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot use REMOTE data location in STANDALONE mode", e.getMessage());
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfigReader() {
    IndexStartConfig.fromConfig(null);
  }
}
