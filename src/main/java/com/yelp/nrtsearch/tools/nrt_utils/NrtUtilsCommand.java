/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils;

import com.yelp.nrtsearch.tools.nrt_utils.backup.CleanupSnapshotsCommand;
import com.yelp.nrtsearch.tools.nrt_utils.backup.ListSnapshotsCommand;
import com.yelp.nrtsearch.tools.nrt_utils.backup.RestoreCommand;
import com.yelp.nrtsearch.tools.nrt_utils.backup.SnapshotCommand;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.DeleteIncrementalSnapshotsCommand;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalDataCleanupCommand;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.ListIncrementalSnapshotsCommand;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.RestoreIncrementalCommand;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.SnapshotIncrementalCommand;
import com.yelp.nrtsearch.tools.nrt_utils.state.GetRemoteStateCommand;
import com.yelp.nrtsearch.tools.nrt_utils.state.GetResourceVersionCommand;
import com.yelp.nrtsearch.tools.nrt_utils.state.ListResourceVersions;
import com.yelp.nrtsearch.tools.nrt_utils.state.PutRemoteStateCommand;
import com.yelp.nrtsearch.tools.nrt_utils.state.SetResourceVersionCommand;
import com.yelp.nrtsearch.tools.nrt_utils.state.UpdateGlobalIndexStateCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "nrt_utils",
    synopsisSubcommandLabel = "COMMAND",
    subcommands = {
      CleanupSnapshotsCommand.class,
      DeleteIncrementalSnapshotsCommand.class,
      GetRemoteStateCommand.class,
      GetResourceVersionCommand.class,
      IncrementalDataCleanupCommand.class,
      ListIncrementalSnapshotsCommand.class,
      ListResourceVersions.class,
      ListSnapshotsCommand.class,
      PutRemoteStateCommand.class,
      RestoreCommand.class,
      RestoreIncrementalCommand.class,
      SetResourceVersionCommand.class,
      SnapshotCommand.class,
      SnapshotIncrementalCommand.class,
      UpdateGlobalIndexStateCommand.class,
      CommandLine.HelpCommand.class
    })
public class NrtUtilsCommand implements Runnable {

  public static void main(String[] args) {
    System.exit(new CommandLine(new NrtUtilsCommand()).execute(args));
  }

  @Override
  public void run() {
    // if only the base command is run, just print the usage
    new CommandLine(this).execute("help");
  }
}
