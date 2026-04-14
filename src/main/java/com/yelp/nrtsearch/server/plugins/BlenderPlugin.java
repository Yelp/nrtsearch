/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.plugins;

import com.yelp.nrtsearch.server.search.multiretriever.blender.BlenderOperation;
import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for providing custom {@link BlenderOperation}s. Provides info for registration
 * of blenders by name. These blenders can be invoked by sending a {@link
 * com.yelp.nrtsearch.server.grpc.PluginBlender} message as part of the {@link
 * com.yelp.nrtsearch.server.grpc.Blender} in a {@link
 * com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest}.
 *
 * <p>Implementations should be stateless; any per-request parameters are available at execution
 * time via the {@link com.yelp.nrtsearch.server.grpc.Blender} proto passed to {@link
 * BlenderOperation#mergeHits}.
 */
public interface BlenderPlugin {
  /** Get map of blender name to {@link BlenderOperation} instance for registration. */
  default Map<String, BlenderOperation> getBlenders() {
    return Collections.emptyMap();
  }
}
