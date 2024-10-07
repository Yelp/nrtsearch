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
package com.yelp.nrtsearch.tools.cli;

import com.yelp.nrtsearch.server.grpc.NrtsearchClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = BackupWarmingQueriesCommand.BACKUP_WARMING_QUERIES,
    description = "Backup warming queries")
public class BackupWarmingQueriesCommand implements Callable<Integer> {
  public static final String BACKUP_WARMING_QUERIES = "backupWarmingQueries";

  @CommandLine.ParentCommand private NrtsearchClientCommand baseCmd;

  @CommandLine.Option(
      names = {"-i", "--index"},
      description = "Name of the index which is to be backed up",
      required = true)
  private String index;

  public String getIndex() {
    return index;
  }

  @CommandLine.Option(
      names = {"-s", "--serviceName"},
      description = "Name of the service which is to be backed up")
  private String serviceName;

  public String getServiceName() {
    return serviceName;
  }

  @CommandLine.Option(
      names = {"-n", "--numQueriesThreshold"},
      description = "Optional; minimum # of queries required to backup warming queries")
  private int numQueriesThreshold;

  public int getNumQueriesThreshold() {
    return numQueriesThreshold;
  }

  @CommandLine.Option(
      names = {"-u", "--uptimeMinutesThreshold"},
      description = "Optional; minimum # of minutes uptime to backup warming queries")
  private int uptimeMinutesThreshold;

  public int getUptimeMinutesThreshold() {
    return uptimeMinutesThreshold;
  }

  @Override
  public Integer call() throws Exception {
    NrtsearchClient client = baseCmd.getClient();
    try {
      client.backupWarmingQueries(
          getIndex(), getServiceName(), getNumQueriesThreshold(), getUptimeMinutesThreshold());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
