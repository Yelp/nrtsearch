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
package com.yelp.nrtsearch.server.cli;

import picocli.CommandLine;

@CommandLine.Command(
    name = BackupIndexCommand.BACKUP_INDEX,
    mixinStandardHelpOptions = true,
    version = "backupIndex 0.1",
    description = "Backup index")
public class BackupIndexCommand {
  public static final String BACKUP_INDEX = "backupIndex";

  @CommandLine.Option(
      names = {"-i", "--indexName"},
      description = "name of the index which is to be backed up",
      required = true)
  private String indexName;

  public String getIndexName() {
    return indexName;
  }

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "name of the service which is to be backed up",
      required = true)
  private String serviceName;

  public String getServiceName() {
    return serviceName;
  }

  @CommandLine.Option(
      names = {"-r", "--resourceName"},
      description = "name of the resource which is to be backed up",
      required = true)
  private String resourceName;

  public String getResourceName() {
    return resourceName;
  }
}
