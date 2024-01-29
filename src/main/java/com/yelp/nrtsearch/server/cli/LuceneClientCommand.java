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

import com.yelp.nrtsearch.server.Version;
import com.yelp.nrtsearch.server.grpc.LuceneServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/*Each new command needs to be added here*/
@CommandLine.Command(
    name = "lucene-client",
    synopsisSubcommandLabel = "COMMAND",
    subcommands = {
      AddDocumentsCommand.class,
      BackupIndexCommand.class,
      BackupWarmingQueriesCommand.class,
      CommitCommand.class,
      CreateIndexCommand.class,
      GetCurrentSearcherVersion.class,
      DeleteDocumentsCommand.class,
      DeleteAllDocumentsCommand.class,
      DeleteIndexBackupCommand.class,
      DeleteIndexCommand.class,
      ForceMergeCommand.class,
      ForceMergeDeletesCommand.class,
      IndicesCommand.class,
      LiveSettingsCommand.class,
      LiveSettingsV2Command.class,
      ReadyCommand.class,
      RefreshCommand.class,
      RegisterFieldsCommand.class,
      ReloadStateCommand.class,
      SearchCommand.class,
      SettingsCommand.class,
      SettingsV2Command.class,
      StartIndexCommand.class,
      StartIndexV2Command.class,
      StatsCommand.class,
      StatusCommand.class,
      StopIndexCommand.class,
      WriteNRTPointCommand.class,
      CommandLine.HelpCommand.class
    })
public class LuceneClientCommand implements Runnable {
  public static final Logger logger = LoggerFactory.getLogger(LuceneClientCommand.class.getName());

  @CommandLine.Option(
      names = {"-p", "--port"},
      description = "Port number of server to connect to (default: ${DEFAULT-VALUE})",
      defaultValue = "6000")
  private String port;

  public int getPort() {
    return Integer.parseInt(port);
  }

  @CommandLine.Option(
      names = {"-h", "--hostname", "--host"},
      description = "Host name of server to connect to (default: ${DEFAULT-VALUE})",
      defaultValue = "localhost")
  private String hostname;

  @CommandLine.Option(
      names = {"-V", "--version"},
      description = "Print version information and exit")
  private boolean printVersion;

  public String getHostname() {
    return hostname;
  }

  public LuceneServerClient getClient() {
    return new LuceneServerClient(getHostname(), getPort());
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new LuceneClientCommand()).execute(args));
  }

  @Override
  public void run() {
    if (printVersion) {
      System.out.println(Version.CURRENT);
    } else {
      // if only the base command is run, just print the usage
      new CommandLine(this).execute("help");
    }
  }
}
