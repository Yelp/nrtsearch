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
package com.yelp.nrtsearch.server.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public interface Archiver {
  Path download(final String serviceName, final String resource) throws IOException;

  /**
   * Upload the directory and files at the provided path. If either filesToInclude or
   * parentDirectoriesToInclude or both are provided only the specified files or files in specified
   * directories or both would be uploaded.
   */
  String upload(
      final String serviceName,
      final String resource,
      Path path,
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException;

  boolean blessVersion(final String serviceName, final String resource, String versionHash)
      throws IOException;

  boolean deleteVersion(final String serviceName, final String resource, String versionHash)
      throws IOException;

  /**
   * Delete all locally cached file associated with the given resource.
   *
   * @param resource resource name
   * @return if files were successfully deleted
   */
  boolean deleteLocalFiles(final String resource);

  List<String> getResources(final String serviceName);

  List<VersionedResource> getVersionedResource(final String serviceName, final String resource);
}
