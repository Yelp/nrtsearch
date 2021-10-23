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
package com.yelp.nrtsearch.server.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class IncrementalArchiverImpl implements Archiver {
  private final BackupDiffManager backupDiffManager;

  public IncrementalArchiverImpl(BackupDiffManager backupDiffManager) {
    this.backupDiffManager = backupDiffManager;
  }

  @Override
  public Path download(String serviceName, String resource) throws IOException {
    return null;
  }

  @Override
  public String upload(
      String serviceName,
      String resource,
      Path path, // path to resource_name/shard0/index/
      Collection<String> filesToInclude,
      Collection<String> parentDirectoriesToInclude,
      boolean stream)
      throws IOException {
    return null;
  }

  @Override
  public boolean blessVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return backupDiffManager.blessVersion(serviceName, resource, versionHash);
  }

  @Override
  public boolean deleteVersion(String serviceName, String resource, String versionHash)
      throws IOException {
    return false;
  }

  @Override
  public List<String> getResources(String serviceName) {
    return null;
  }

  @Override
  public List<VersionedResource> getVersionedResource(String serviceName, String resource) {
    return null;
  }
}
