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

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.backup.*;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = RestoreHelper.BACKUP, description = "Restore index")
public class RestoreHelper implements Callable<Integer> {
  public static final String BACKUP = "restore";
  @CommandLine.ParentCommand private BackupRestoreCommand baseCmd;
  private static final Logger logger = LoggerFactory.getLogger(RestoreHelper.class.getName());

  @CommandLine.Option(
      names = {"-l", "--legacy"},
      description = "Use legacy version for restore")
  private boolean legacy;

  @Override
  public Integer call() throws Exception {
    if (legacy) {
      AmazonS3 amazonS3 = baseCmd.getAmazonS3();
      Archiver archiver =
          new ArchiverImpl(
              amazonS3,
              baseCmd.getBucket(),
              Paths.get(baseCmd.getArchiveDir(), UUID.randomUUID().toString()),
              new TarImpl(Tar.CompressionMode.LZ4),
              true);
      restore(archiver);
    } else {
      Archiver archiver = baseCmd.getArchiver();
      restore(archiver);
    }
    return 0;
  }

  public void restore(Archiver archiver) throws IOException {
    long t1 = System.nanoTime();
    Path downloadPath =
        archiver.download(
            baseCmd.getServiceName(), IndexBackupUtils.getResourceData(baseCmd.getResourceName()));
    long t2 = System.nanoTime();
    logger.info(
        String.format(
            "Downloaded service: %s, resource: %s to path: %s. Time taken %s milliseconds",
            baseCmd.getServiceName(),
            baseCmd.getResourceName(),
            downloadPath.toAbsolutePath(),
            (t2 - t1) / (1000 * 1000)));
    t1 = System.nanoTime();
    downloadPath =
        archiver.download(
            baseCmd.getServiceName(),
            IndexBackupUtils.getResourceMetadata(baseCmd.getResourceName()));
    t2 = System.nanoTime();
    logger.info(
        "Downloaded service: %s, resource: %s to path: %s. Time taken %s milliseconds",
        baseCmd.getServiceName(),
        baseCmd.getResourceName(),
        downloadPath.toAbsolutePath(),
        (t2 - t1) / (1000 * 1000));
  }
}
