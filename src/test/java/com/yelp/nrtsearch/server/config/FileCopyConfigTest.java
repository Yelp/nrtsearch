/*
 * Copyright 2021 Yelp Inc.
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

import java.io.ByteArrayInputStream;
import org.junit.Test;

public class FileCopyConfigTest {
  private static FileCopyConfig getConfig(String configFile) {
    return FileCopyConfig.fromConfig(
        new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
  }

  @Test
  public void testDefault() {
    String configFile = "nodeName: \"server_foo\"";
    FileCopyConfig config = getConfig(configFile);
    assertFalse(config.getAckedCopy());
    assertEquals(FileCopyConfig.DEFAULT_CHUNK_SIZE, config.getChunkSize());
    assertEquals(FileCopyConfig.DEFAULT_ACK_EVERY, config.getAckEvery());
    assertEquals(FileCopyConfig.DEFAULT_MAX_IN_FLIGHT, config.getMaxInFlight());
  }

  @Test
  public void testConfig() {
    String configFile =
        String.join(
            "\n",
            "nodeName: \"server_foo\"",
            "FileCopyConfig:",
            "  ackedCopy: true",
            "  chunkSize: 100",
            "  ackEvery: 10",
            "  maxInFlight: 1000");
    FileCopyConfig config = getConfig(configFile);
    assertTrue(config.getAckedCopy());
    assertEquals(100, config.getChunkSize());
    assertEquals(10, config.getAckEvery());
    assertEquals(1000, config.getMaxInFlight());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidAckEvery() {
    new FileCopyConfig(true, 100, 1000, 10);
  }
}
