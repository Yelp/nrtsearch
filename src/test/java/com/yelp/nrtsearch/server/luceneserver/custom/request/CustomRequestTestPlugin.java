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

import com.yelp.nrtsearch.server.plugins.CustomRequestPlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.util.Map;

public class CustomRequestTestPlugin extends Plugin implements CustomRequestPlugin {

  @Override
  public String id() {
    return "test_custom_request_plugin";
  }

  @Override
  public Map<String, RequestProcessor> getRoutes() {
    return Map.of(
        "test_path",
        (path, request) ->
            Map.of("result", String.format("%s: %s world!", path, request.get("test_key"))));
  }
}
