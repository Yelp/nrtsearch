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
package com.yelp.nrtsearch.server.index;

public class RemoteFilePath {
  private final String path;
  private static final String SLASH_REGEX = "^/+";

  public RemoteFilePath(String path) {
    this.path = preprocessPath(path);
  }

  public String getPath() {
    return this.path;
  }

  @Override
  public String toString() {
    return path;
  }

  private static String preprocessPath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("File Path cannot be null");
    }
    String trimmedPath = (path.trim()).replaceFirst(SLASH_REGEX, "");
    if (!trimmedPath.isEmpty() && !trimmedPath.endsWith("/")) {
      trimmedPath += "/";
    }
    return trimmedPath;
  }
}
