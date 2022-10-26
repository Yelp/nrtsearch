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

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.CustomRequest;
import com.yelp.nrtsearch.server.grpc.CustomResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import com.yelp.nrtsearch.server.plugins.Plugin;
import io.grpc.testing.GrpcCleanupRule;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class CustomRpcTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  protected List<Plugin> getPlugins(LuceneServerConfiguration configuration) {
    return List.of(new CustomRequestTestPlugin());
  }

  @Test
  public void testCustomRequest() {
    CustomRequest request =
        CustomRequest.newBuilder()
            .setId("test_custom_request_plugin")
            .setPath("test_path")
            .putParams("test_key", "hello")
            .build();

    CustomResponse response = getGrpcServer().getBlockingStub().custom(request);

    assertEquals(Map.of("result", "test_path: hello world!"), response.getResponseMap());
  }
}
