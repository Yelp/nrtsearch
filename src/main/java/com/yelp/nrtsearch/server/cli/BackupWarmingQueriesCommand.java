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

import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = BackupWarmingQueriesCommand.BACKUP_WARMING_QUERIES,
    description = "Backup warming queries")
public class BackupWarmingQueriesCommand implements Callable<Integer> {
  public static final String BACKUP_WARMING_QUERIES = "backupWarmingQueries";

  @CommandLine.ParentCommand private LuceneClientCommand baseCmd;

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

  @Override
  public Integer call() throws Exception {
    LuceneServerClient client = baseCmd.getClient();
    try {
      client.backupWarmingQueries(getIndex(), getServiceName());
    } finally {
      client.shutdown();
    }
    return 0;
  }
}
