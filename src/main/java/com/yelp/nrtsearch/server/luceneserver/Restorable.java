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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Restorable {
  String TMP_SUFFIX = ".tmp";
  Logger logger = LoggerFactory.getLogger(Restorable.class);

  default void restoreDir(Path source, Path target) throws IOException {
    Path baseSource = source.getParent();
    Path tempCurrentLink = baseSource.resolve(getTmpName());
    Path downloadedFileName = Files.list(source).findFirst().get().getFileName();
    Path downloadedData = source.resolve(downloadedFileName);
    logger.info("Point symlink " + target.toString() + " to " + downloadedData.toString());
    Files.createSymbolicLink(tempCurrentLink, downloadedData);
    cleanupEmptyStateIfExists(target);
    Files.move(tempCurrentLink, target, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * @param target Path to a directory
   * @throws IOException, DirectoryNotEmptyException
   */
  private void cleanupEmptyStateIfExists(Path target) throws IOException {
    // in case this is an index dir it might have empty state dir created while initializing
    // IndexState object
    Path stateDir = target.resolve("state");
    if (Files.exists(stateDir)) {
      Files.delete(stateDir);
    }
  }

  default String getTmpName() {
    return UUID.randomUUID().toString() + TMP_SUFFIX;
  }
}
