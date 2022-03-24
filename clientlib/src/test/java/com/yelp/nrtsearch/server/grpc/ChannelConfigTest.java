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
package com.yelp.nrtsearch.server.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.grpc.ChannelConfig.ServiceConfig.MethodConfig;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ChannelConfigTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testEmptyConfig() throws IOException {
    String configJson = "{}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    verifyNoMoreInteractions(mockBuilder);
  }

  @Test
  public void testEnableRetry() throws IOException {
    String configJson = "{\"enableRetry\": true}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    verify(mockBuilder, times(1)).enableRetry();
    verifyNoMoreInteractions(mockBuilder);
  }

  @Test
  public void testDisableRetry() throws IOException {
    String configJson = "{\"enableRetry\": false}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    verify(mockBuilder, times(1)).disableRetry();
    verifyNoMoreInteractions(mockBuilder);
  }

  @Test
  public void testMaxHedgedAttempts() throws IOException {
    String configJson = "{\"maxHedgedAttempts\": 5}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    verify(mockBuilder, times(1)).maxHedgedAttempts(5);
    verifyNoMoreInteractions(mockBuilder);
  }

  @Test
  public void testMaxInboundMessageSize() throws IOException {
    String configJson = "{\"maxInboundMessageSize\": 8388608}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    verify(mockBuilder, times(1)).maxInboundMessageSize(8388608);
    verifyNoMoreInteractions(mockBuilder);
  }

  @Test(expected = NullPointerException.class)
  public void testNullMethodName() {
    new MethodConfig(null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoMethodName() {
    new MethodConfig(Collections.emptyList(), null, null);
  }

  @Test
  public void testEmptyName() throws IOException {
    String configJson = "{\"serviceConfig\": {\"methodConfig\": [{\"name\": [{}]}]}}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    Map<String, Object> expectedServiceConfig = new HashMap<>();
    Map<String, Object> expectedMethodConfig = new HashMap<>();
    expectedMethodConfig.put("name", Collections.singletonList(new HashMap<String, Object>()));
    expectedServiceConfig.put("methodConfig", Collections.singletonList(expectedMethodConfig));

    verify(mockBuilder, times(1)).defaultServiceConfig(expectedServiceConfig);
    verifyNoMoreInteractions(mockBuilder);
    verifyServiceConfigSettable(expectedServiceConfig);
  }

  @Test
  public void testMultiName() throws IOException {
    String configJson =
        "{\"serviceConfig\": {\"methodConfig\": [{\"name\": [{\"service\": \"s1\", \"method\": \"m1\"}, {\"service\": \"s2\", \"method\": \"m2\"}]}]}}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    Map<String, Object> expectedServiceConfig = new HashMap<>();
    Map<String, Object> expectedMethodConfig = new HashMap<>();
    Map<String, Object> expectedName1 = new HashMap<>();
    expectedName1.put("service", "s1");
    expectedName1.put("method", "m1");
    Map<String, Object> expectedName2 = new HashMap<>();
    expectedName2.put("service", "s2");
    expectedName2.put("method", "m2");
    expectedMethodConfig.put("name", Arrays.asList(expectedName1, expectedName2));
    expectedServiceConfig.put("methodConfig", Collections.singletonList(expectedMethodConfig));

    verify(mockBuilder, times(1)).defaultServiceConfig(expectedServiceConfig);
    verifyNoMoreInteractions(mockBuilder);
    verifyServiceConfigSettable(expectedServiceConfig);
  }

  @Test
  public void testMethodTimeout() throws IOException {
    String configJson =
        "{\"serviceConfig\": {\"methodConfig\": [{\"name\": [{}], \"timeout\": \"2s\"}]}}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    Map<String, Object> expectedServiceConfig = new HashMap<>();
    Map<String, Object> expectedMethodConfig = new HashMap<>();
    expectedMethodConfig.put("name", Collections.singletonList(new HashMap<String, Object>()));
    expectedMethodConfig.put("timeout", "2s");
    expectedServiceConfig.put("methodConfig", Collections.singletonList(expectedMethodConfig));

    verify(mockBuilder, times(1)).defaultServiceConfig(expectedServiceConfig);
    verifyNoMoreInteractions(mockBuilder);
    verifyServiceConfigSettable(expectedServiceConfig);
  }

  @Test
  public void testMethodHedgingPolicy() throws IOException {
    String configJson =
        "{\"serviceConfig\": {\"methodConfig\": [{\"name\": [{}], \"hedgingPolicy\": {\"maxAttempts\": 5, \"hedgingDelay\": \"1s\", \"nonFatalStatusCodes\": [\"UNAVAILABLE\"]}}]}}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    Map<String, Object> expectedServiceConfig = new HashMap<>();
    Map<String, Object> expectedMethodConfig = new HashMap<>();
    Map<String, Object> expectedHedgingConfig = new HashMap<>();
    expectedMethodConfig.put("name", Collections.singletonList(new HashMap<String, Object>()));
    expectedHedgingConfig.put("maxAttempts", 5.0);
    expectedHedgingConfig.put("hedgingDelay", "1s");
    expectedHedgingConfig.put("nonFatalStatusCodes", Collections.singletonList("UNAVAILABLE"));
    expectedMethodConfig.put("hedgingPolicy", expectedHedgingConfig);
    expectedServiceConfig.put("methodConfig", Collections.singletonList(expectedMethodConfig));

    verify(mockBuilder, times(1)).defaultServiceConfig(expectedServiceConfig);
    verifyNoMoreInteractions(mockBuilder);
    verifyServiceConfigSettable(expectedServiceConfig);
  }

  @Test
  public void testRetryThrottling() throws IOException {
    String configJson =
        "{\"serviceConfig\": {\"retryThrottling\": {\"maxTokens\": 10, \"tokenRatio\": 0.12}}}";
    ChannelConfig config = OBJECT_MAPPER.readValue(configJson, ChannelConfig.class);

    ManagedChannelBuilder<?> mockBuilder = mock(ManagedChannelBuilder.class);
    config.configureChannelBuilder(mockBuilder, OBJECT_MAPPER);

    Map<String, Object> expectedServiceConfig = new HashMap<>();
    Map<String, Object> expectedThrottlingConfig = new HashMap<>();
    expectedThrottlingConfig.put("maxTokens", 10.0);
    expectedThrottlingConfig.put("tokenRatio", 0.12);
    expectedServiceConfig.put("retryThrottling", expectedThrottlingConfig);

    verify(mockBuilder, times(1)).defaultServiceConfig(expectedServiceConfig);
    verifyNoMoreInteractions(mockBuilder);
    verifyServiceConfigSettable(expectedServiceConfig);
  }

  private void verifyServiceConfigSettable(Map<String, Object> serviceConfig) {
    // setting service config validates typing
    ManagedChannelBuilder.forAddress("host", 1111).defaultServiceConfig(serviceConfig);
  }
}
