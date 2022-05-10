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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;

public class IndexBackupUtils {
  public static final String INDEX_STATE_SUFFIX = "-state";

  public static String getResourceMetadata(String resourceName) {
    return String.format("%s_metadata", resourceName);
  }

  public static String getResourceData(String resourceName) {
    return String.format("%s_data", resourceName);
  }

  public static String getResourceVersionMetadata(String resourceName) {
    return String.format("_version/%s_metadata", resourceName);
  }

  public static String getResourceVersionData(String resourceName) {
    return String.format("_version/%s_data", resourceName);
  }

  public static boolean isMetadata(String resourceName) {
    return resourceName.contains("_metadata");
  }

  public static boolean isBackendGlobalState(String resourceName) {
    return RemoteStateBackend.GLOBAL_STATE_RESOURCE.equals(resourceName);
  }

  /** Get if this resource applies for index state. */
  public static boolean isIndexState(String resourceName) {
    return resourceName.endsWith(INDEX_STATE_SUFFIX);
  }
}
