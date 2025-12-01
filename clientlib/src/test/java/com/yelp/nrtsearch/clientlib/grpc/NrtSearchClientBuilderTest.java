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
package com.yelp.nrtsearch.clientlib.grpc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.ChannelConfig;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.lang.reflect.Field;
import org.junit.Test;

public class NrtSearchClientBuilderTest {
  private static final String HOST = "localhost";
  private static final int PORT = 9999;

  @Test
  public void testBuilderWithManagedChannelBuilder() {
    // Create a builder with ManagedChannelBuilder
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(HOST, PORT);
    NrtSearchClient.Builder builder = new NrtSearchClient.Builder(channelBuilder);
    assertNotNull(builder);

    // Build the client
    NrtSearchClient client = builder.build();
    assertNotNull(client);
  }

  @Test
  public void testBuilderSetObjectMapper() throws NoSuchFieldException, IllegalAccessException {
    // Create a custom ObjectMapper
    ObjectMapper customMapper = new ObjectMapper();

    // Create a builder with ManagedChannelBuilder
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(HOST, PORT);
    NrtSearchClient.Builder builder = new NrtSearchClient.Builder(channelBuilder);

    // Set the custom ObjectMapper
    builder.setObjectMapper(customMapper);

    // Use reflection to verify the ObjectMapper was set
    Field objectMapperField = NrtSearchClient.Builder.class.getDeclaredField("objectMapper");
    objectMapperField.setAccessible(true);
    ObjectMapper actualMapper = (ObjectMapper) objectMapperField.get(builder);

    // Verify the ObjectMapper is the same instance
    assertSame(customMapper, actualMapper);
  }

  @Test
  public void testBuilderSetChannelConfig() throws IOException {
    // Create a simple ChannelConfig
    String configJson = "{\"enableRetry\": true}";
    ObjectMapper objectMapper = new ObjectMapper();
    ChannelConfig config = objectMapper.readValue(configJson, ChannelConfig.class);

    // Create a builder with ManagedChannelBuilder
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(HOST, PORT);
    NrtSearchClient.Builder builder = new NrtSearchClient.Builder(channelBuilder);

    // Set the ChannelConfig
    builder.setChannelConfig(config);

    // Build the client
    NrtSearchClient client = builder.build();
    assertNotNull(client);
  }
}
