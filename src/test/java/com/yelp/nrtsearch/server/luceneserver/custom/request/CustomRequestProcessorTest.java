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
package com.yelp.nrtsearch.server.luceneserver.custom.request;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class CustomRequestProcessorTest {

  @BeforeClass
  public static void initCustomRequestProcessor() {
    CustomRequestProcessor.initialize(null, List.of(new CustomRequestTestPlugin()));
  }

  @Test
  public void testValidRequest() {
    CustomRequest request =
        CustomRequest.newBuilder()
            .setId("test_custom_request_plugin")
            .setPath("test_path")
            .putParams("test_key", "hello")
            .build();
    CustomResponse response = CustomRequestProcessor.processCustomRequest(request);

    assertEquals(Map.of("result", "test_path: hello world!"), response.getResponseMap());
  }

  @Test(expected = RouteNotFoundException.class)
  public void testInvalidId() {
    CustomRequest request =
        CustomRequest.newBuilder()
            .setId("invalid_id")
            .setPath("test_path")
            .putParams("test_key", "hello")
            .build();
    try {
      CustomRequestProcessor.processCustomRequest(request);
    } catch (RouteNotFoundException e) {
      assertEquals("No routes found for id invalid_id", e.getMessage());
      throw e;
    }
  }

  @Test(expected = RouteNotFoundException.class)
  public void testInvalidPath() {
    CustomRequest request =
        CustomRequest.newBuilder()
            .setId("test_custom_request_plugin")
            .setPath("invalid_path")
            .putParams("test_key", "hello")
            .build();
    try {
      CustomRequestProcessor.processCustomRequest(request);
    } catch (RouteNotFoundException e) {
      assertEquals("Path invalid_path not found for id test_custom_request_plugin", e.getMessage());
      throw e;
    }
  }

  @Test(expected = DuplicateRouteException.class)
  public void testDuplicatePluginId() {
    List<Plugin> plugins = List.of(new CustomRequestTestPlugin(), new CustomRequestTestPlugin());
    try {
      CustomRequestProcessor.initialize(null, plugins);
    } catch (DuplicateRouteException e) {
      assertEquals(
          "Multiple custom request plugins with id test_custom_request_plugin found, please have unique ids in CustomRequestPlugin",
          e.getMessage());
      throw e;
    }
  }
}
