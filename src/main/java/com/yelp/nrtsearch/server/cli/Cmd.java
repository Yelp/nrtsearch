/*
 *
 *  * Copyright 2019 Yelp Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  * either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.yelp.nrtsearch.server.cli;

import picocli.CommandLine;

/*Each new command needs to be added here*/
@CommandLine.Command(name = "cmd", synopsisSubcommandLabel = "COMMAND", subcommands = {
        CreateIndexCommand.class,
        LiveSettingsCommand.class,
        RegisterFieldsCommand.class,
        SettingsCommand.class,
        StartIndexCommand.class,
        AddDocumentsCommand.class,
        RefreshCommand.class,
        CommitCommand.class,
        StatsCommand.class,
        SearchCommand.class,
        DeleteDocumentsCommand.class,
        DeleteAllDocumentsCommand.class,
        DeleteIndexCommand.class,
        StopIndexCommand.class,
        WriteNRTPointCommand.class,
        GetCurrentSearcherVersion.class,
        BackupIndexCommand.class,
        StatusCommand.class
})
public class Cmd {
    @CommandLine.Option(names = {"-p", "--port"}, description = "port number of server to connect to", required = true)
    private String port;

    public int getPort() {
        return Integer.parseInt(port);
    }

    @CommandLine.Option(names = {"-h", "--hostname"}, description = "host name of server to connect to", required = true)
    private String hostname;

    public String getHostname() {
        return hostname;
    }

}
