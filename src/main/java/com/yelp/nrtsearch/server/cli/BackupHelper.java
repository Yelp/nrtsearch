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
package com.yelp.nrtsearch.server.cli;

import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.IndexArchiver;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = BackupHelper.BACKUP, description = "Backup index")
public class BackupHelper implements Callable<Integer> {
  public static final String BACKUP = "backup";
  @CommandLine.ParentCommand private BackupRestoreCommand baseCmd;
  private static final Logger logger = LoggerFactory.getLogger(BackupHelper.class.getName());

  @CommandLine.Option(
      names = {"-i", "--index_dir"},
      description = "Path to directory containing current index on disk",
      required = true)
  private String indexDir;

  public String getIndexDir() {
    return indexDir;
  }

  @CommandLine.Option(
      names = {"-s", "--state_dir"},
      description = "Path to directory containing current global state on disk",
      required = true)
  private String stateDir;

  public String getStateDir() {
    return stateDir;
  }

  @Override
  public Integer call() throws Exception {
    Archiver archiver = baseCmd.getArchiver();
    List<String> files =
        Files.list(IndexArchiver.getIndexDataDir(Path.of(this.getIndexDir())))
            .filter(Files::isRegularFile)
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toList());
    long t1 = System.nanoTime();
    String versionHash =
        archiver.upload(
            baseCmd.getServiceName(),
            IndexBackupUtils.getResourceData(baseCmd.getResourceName()),
            Path.of(this.getIndexDir()),
            files,
            Collections.emptyList(),
            true);
    long t2 = System.nanoTime();
    logger.info(
        String.format(
            "Time taken to upload data %s milliseconds, versionHash uploaded: %s",
            (t2 - t1) / (1000 * 1000), versionHash));
    boolean result =
        archiver.blessVersion(
            baseCmd.getServiceName(),
            IndexBackupUtils.getResourceData(baseCmd.getResourceName()),
            versionHash);
    t1 = System.nanoTime();
    String versionHashMetadata =
        archiver.upload(
            baseCmd.getServiceName(),
            IndexBackupUtils.getResourceMetadata(baseCmd.getResourceName()),
            Path.of(this.getStateDir()),
            Collections.emptyList(),
            Collections.emptyList(),
            true);
    t2 = System.nanoTime();
    logger.info(
        "Time taken to upload data %s milliseconds, versionHash uploaded: %s",
        (t2 - t1) / (1000 * 1000), versionHashMetadata);
    boolean resultMetadata =
        archiver.blessVersion(
            baseCmd.getServiceName(),
            IndexBackupUtils.getResourceMetadata(baseCmd.getResourceName()),
            versionHashMetadata);
    if (result && resultMetadata) {
      logger.info(
          String.format(
              "Blessed, service: %s, resource: %s, version: %s",
              baseCmd.getServiceName(),
              IndexBackupUtils.getResourceData(baseCmd.getResourceName()),
              versionHash));
      logger.info(
          String.format(
              "Blessed, service: %s, resource: %s, version: %s",
              baseCmd.getServiceName(),
              IndexBackupUtils.getResourceMetadata(baseCmd.getResourceName()),
              versionHashMetadata));
    } else {
      logger.info(
          String.format(
              "Failed to bless: service: %s, resource: %s, version: %s",
              baseCmd.getServiceName(),
              IndexBackupUtils.getResourceData(baseCmd.getResourceName()),
              versionHash));
    }
    return 0;
  }
}
